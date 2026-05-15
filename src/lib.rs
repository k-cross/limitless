use core::mem::MaybeUninit;
#[cfg(not(loom))]
use {
    core::cell::UnsafeCell,
    core::hint::spin_loop,
    core::sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
#[cfg(loom)]
use {
    loom::cell::UnsafeCell,
    loom::hint::spin_loop,
    loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Debug)]
struct Slot<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    initialized: AtomicBool,
}

/// Implementation of a lock-free ring buffer that takes a fixed and unchangable
/// size of items to store.
#[derive(Debug)]
pub struct RingBuffer<T> {
    buffer: Box<[Slot<T>]>,
    capacity: usize,
    read_idx: AtomicUsize,
    write_idx: AtomicUsize,
}

unsafe impl<T> Send for RingBuffer<T> {}
unsafe impl<T> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let buffer: Box<[Slot<T>]> = (0..capacity)
            .map(|_| Slot {
                data: UnsafeCell::new(MaybeUninit::uninit()),
                initialized: AtomicBool::new(false),
            })
            .collect();
        Self {
            buffer,
            capacity,
            read_idx: AtomicUsize::new(0),
            write_idx: AtomicUsize::new(0),
        }
    }

    pub fn is_full(&self) -> bool {
        (self.write_idx.load(Ordering::Acquire) + 1) % self.capacity
            == self.read_idx.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.write_idx.load(Ordering::Acquire) == self.read_idx.load(Ordering::Acquire)
    }

    pub fn read(&self) -> Result<T, ()> {
        let rr: T;
        loop {
            let idx = self.read_idx.load(Ordering::Acquire);
            if self.is_empty() {
                return Err(());
            }
            if !self.buffer[idx].initialized.load(Ordering::Acquire) {
                // spin until initialized
                spin_loop();
                continue;
            }
            // a pause here could cause uninitialized memory reads on a full loop
            if self
                .read_idx
                .compare_exchange_weak(
                    idx,
                    (idx + 1) % self.capacity,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                spin_loop();
                continue;
            };
            // SAFETY:
            // - index is unique given the time checked
            cfg_if::cfg_if! {
                if #[cfg(loom)] {
                    rr = unsafe {self.buffer[idx].data.with(|ptr| core::ptr::read(ptr).assume_init())};
                } else {
                    rr = unsafe { core::ptr::read(self.buffer[idx].data.get()).assume_init() };
                }
            };
            self.buffer[idx].initialized.store(false, Ordering::Release);
            break;
        }
        Ok(rr)
    }

    pub fn write(&self, v: T) -> Result<(), ()> {
        loop {
            let idx = self.write_idx.load(Ordering::Acquire);
            if self.buffer[idx].initialized.load(Ordering::Acquire) {
                // spin until uninitialized
                spin_loop();
                continue;
            }
            if self.is_full() {
                return Err(());
            }
            if self
                .write_idx
                .compare_exchange_weak(
                    idx,
                    (idx + 1) % self.capacity,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                // Note: maybe need to add jitter/backoff here
                spin_loop();
                continue;
            };
            // SAFETY:
            // - index is unique given the time checked
            cfg_if::cfg_if! {
                if #[cfg(loom)] {
                    unsafe { self.buffer[idx].data.with_mut(|ptr| core::ptr::write(ptr, MaybeUninit::new(v))) }
                } else {
                    unsafe { core::ptr::write(self.buffer[idx].data.get(), MaybeUninit::new(v)) }
                }
            };
            self.buffer[idx].initialized.store(true, Ordering::Release);
            break;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[cfg(loom)]
    use {loom::sync::Arc, loom::thread};
    #[cfg(not(loom))]
    use {std::sync::Arc, std::thread};

    #[allow(dead_code)]
    enum WishyWashy {
        No,
        Yes,
        Maybe,
    }

    #[allow(dead_code)]
    struct DataCorruptor {
        val1: HashMap<String, Vec<usize>>,
        val2: WishyWashy,
    }

    #[test]
    fn test_single_threaded() {
        // Sequential Access
        const SIZE: usize = 5;
        let rb: RingBuffer<usize> = RingBuffer::new(SIZE);
        for i in 0..(SIZE - 1) {
            assert_eq!(Ok(()), rb.write(i));
        }
        assert_eq!(Err(()), rb.write(6));
        for i in 0..(SIZE - 1) {
            assert_eq!(Ok(i), rb.read());
        }
        assert_eq!(Err(()), rb.read());
    }

    #[test]
    #[cfg(not(loom))]
    fn test_multi_threaded_independent_read_and_write() {
        const SIZE: usize = 1024 * 16;
        let logical_cores: usize = thread::available_parallelism().unwrap().into();
        // needs to be EVEN for test assumptions to work
        let iteration_size = if logical_cores % 2 == 0 {
            logical_cores
        } else {
            logical_cores + 1
        };
        println!("running {iteration_size} threads");
        let rb: Arc<RingBuffer<usize>> = Arc::new(RingBuffer::new(SIZE));
        let sum = Arc::new(AtomicUsize::new(0));
        let sum_err = Arc::new(AtomicUsize::new(0));
        let mut threads = vec![];

        // read threads === write threads
        for i in 0..iteration_size {
            let rbc = rb.clone();
            let s = sum.clone();
            let t = if i % 2 == 0 {
                let se = sum_err.clone();
                thread::spawn(move || {
                    println!("write enter");
                    for _ in 0..SIZE {
                        // complete regardless of contention
                        if let Err(_) = rbc.write(1) {
                            se.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    println!("write exit");
                })
            } else {
                thread::spawn(move || {
                    // always empty the ring buffer but it might be better to
                    // move this to the main thread
                    println!("read enter");
                    while !rbc.is_empty() {
                        if let Ok(r) = rbc.read() {
                            s.fetch_add(r, Ordering::SeqCst);
                        }
                    }
                    println!("read exit");
                })
            };
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        while !rb.is_empty() {
            if let Ok(r) = rb.read() {
                sum.fetch_add(r, Ordering::SeqCst);
            }
        }
        let result = (SIZE * iteration_size / 2) - sum_err.load(Ordering::Acquire);
        println!("result is {result}");
        assert_eq!(sum.load(Ordering::SeqCst), result);
    }

    #[test]
    #[cfg(not(loom))]
    fn test_multi_threaded_independent_data_corruption_check() {
        const SIZE: usize = 1024 * 16;
        let logical_cores: usize = thread::available_parallelism().unwrap().into();
        // needs to be EVEN for test assumptions to work
        let iteration_size = if logical_cores % 2 == 0 {
            logical_cores
        } else {
            logical_cores + 1
        };
        println!("running {iteration_size} threads");
        let rb: Arc<RingBuffer<DataCorruptor>> = Arc::new(RingBuffer::new(SIZE));
        let result = Arc::new(AtomicUsize::new(0));
        let sum = Arc::new(AtomicUsize::new(0));
        let sum_err = Arc::new(AtomicUsize::new(0));
        let mut threads = vec![];

        // read threads === write threads
        for i in 0..iteration_size {
            let rbc = rb.clone();
            let s = sum.clone();
            let t = if i % 2 == 0 {
                let r = result.clone();
                let se = sum_err.clone();
                thread::spawn(move || {
                    println!("write enter");
                    for i in 0..SIZE {
                        // complete regardless of contention
                        let mut hm = HashMap::new();
                        r.fetch_add(i, Ordering::AcqRel);
                        hm.insert("hello".to_owned(), vec![i, i + 1, i + 2]);
                        let w = match i {
                            n if n % 2 == 0 => WishyWashy::Yes,
                            n if n % 3 == 0 => WishyWashy::No,
                            _ => WishyWashy::Maybe,
                        };
                        if rbc.write(DataCorruptor { val1: hm, val2: w }).is_err() {
                            se.fetch_add(1, Ordering::SeqCst);
                            r.fetch_sub(i, Ordering::AcqRel);
                        }
                    }
                    println!("write exit");
                })
            } else {
                thread::spawn(move || {
                    // always empty the ring buffer but it might be better to
                    // move this to the main thread
                    println!("read enter");
                    while !rbc.is_empty() {
                        if let Ok(r) = rbc.read() {
                            s.fetch_add(r.val1["hello"][0], Ordering::SeqCst);
                        }
                    }
                    println!("read exit");
                })
            };
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        while !rb.is_empty() {
            if let Ok(r) = rb.read() {
                sum.fetch_add(r.val1["hello"][0], Ordering::AcqRel);
            }
        }
        let r = result.load(Ordering::Acquire);
        println!("result is {r}");
        assert_eq!(sum.load(Ordering::SeqCst), r);
    }

    #[test]
    #[cfg(loom)]
    fn test_multi_threaded_loom() {
        use std::collections::HashSet;
        loom::model(|| {
            const SIZE: usize = 4;
            let rb: Arc<RingBuffer<usize>> = Arc::new(RingBuffer::new(SIZE));

            let rbd = rb.clone();
            let rbe = rb.clone();

            let t1 = thread::spawn(move || {
                let mut hs: HashSet<usize> = HashSet::new();
                for _ in 0..SIZE {
                    if let Ok(r) = rbd.read() {
                        hs.insert(r);
                    }
                }
                hs
            });

            let t2 = thread::spawn(move || {
                let mut hs: HashSet<usize> = HashSet::new();
                for _ in 0..SIZE {
                    if let Ok(r) = rbe.read() {
                        hs.insert(r);
                    }
                }
                hs
            });

            for i in 0..(SIZE) {
                let _ = rb.write(i);
            }

            let hs_1 = t1.join().unwrap();
            let hs_2 = t2.join().unwrap();
            assert!(hs_1.is_disjoint(&hs_2));
        });
    }
}
