pub use usdt::register_probes;

use core::mem::MaybeUninit;
use crossbeam::utils::CachePadded;
use std::error::Error;
use std::fmt;
#[cfg(not(loom))]
use {
    core::cell::UnsafeCell,
    core::hint::spin_loop,
    core::sync::atomic::{AtomicUsize, Ordering},
};
#[cfg(loom)]
use {
    loom::cell::UnsafeCell,
    loom::lazy_static::yield_now,
    loom::sync::atomic::{AtomicUsize, Ordering},
};

#[usdt::provider]
mod limitless_probes {
    fn read__start(_: &usdt::UniqueId) {}
    fn read__done(_: &usdt::UniqueId, idx: u64, ok: u8) {}
    fn write__start(_: &usdt::UniqueId) {}
    fn write__done(_: &usdt::UniqueId, idx: u64, ok: u8) {}
}

#[derive(Debug, PartialEq)]
pub enum RingBufferError {
    Full,
    Empty,
}

impl fmt::Display for RingBufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RingBufferError::Empty => write!(f, "There are no items"),
            RingBufferError::Full => write!(f, "The buffer is full"),
        }
    }
}

impl Error for RingBufferError {}

#[derive(Debug)]
struct Slot<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    stamp: AtomicUsize,
}

/// Implementation of a lock-free ring buffer that takes a fixed and unchangable
/// size of items to store.
#[derive(Debug)]
pub struct RingBuffer<T> {
    buffer: Box<[Slot<T>]>,
    capacity: CachePadded<usize>,
    read_idx: CachePadded<AtomicUsize>,
    write_idx: CachePadded<AtomicUsize>,
    mcb: CachePadded<usize>,
}

unsafe impl<T> Send for RingBuffer<T> {}
unsafe impl<T> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let buffer: Box<[Slot<T>]> = (0..capacity)
            .map(|i| Slot {
                data: UnsafeCell::new(MaybeUninit::uninit()),
                stamp: AtomicUsize::new(i + 1),
            })
            .collect();
        Self {
            buffer,
            capacity: CachePadded::new(capacity),
            mcb: CachePadded::new((capacity + 1).next_power_of_two()),
            read_idx: CachePadded::new(AtomicUsize::new(0)),
            write_idx: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    pub fn is_full(&self) -> bool {
        let w = self.write_idx.load(Ordering::Acquire);
        let r = self.read_idx.load(Ordering::Acquire);
        let mask = *self.mcb - 1;
        (w & mask) == (r & mask) && r != w
    }

    pub fn is_empty(&self) -> bool {
        self.write_idx.load(Ordering::Acquire) == self.read_idx.load(Ordering::Acquire)
    }

    // private full reducing atomic operations to compared indices
    fn full(&self, r: usize, w: usize) -> bool {
        let mask = *self.mcb - 1;
        (w & mask) == (r & mask) && r != w
    }

    // private empty reducing atomic operations to compared indices
    fn empty(&self, r: usize, w: usize) -> bool {
        w == r
    }

    pub fn read(&self) -> Result<T, RingBufferError> {
        let probe_id = usdt::UniqueId::new();
        limitless_probes::read__start!(|| &probe_id);
        let rr: T;
        loop {
            let idx = self.read_idx.load(Ordering::Acquire);
            let i = idx & (*self.mcb - 1);
            // save true or false to 0 or 1 in branchless computation
            let at_capacity = (i + 1 >= *self.capacity) as usize;
            let new_idx = ((idx + 1) & at_capacity.wrapping_sub(1))
                | (at_capacity * ((idx & *self.mcb) ^ *self.mcb));
            if self.buffer[i].stamp.load(Ordering::Acquire) != idx {
                let widx = self.write_idx.load(Ordering::Acquire);
                if self.empty(idx, widx) {
                    limitless_probes::read__done!(|| (&probe_id, i as u64, 0u8));
                    return Err(RingBufferError::Empty);
                }
                // spin until initialized
                cfg_if::cfg_if! {
                    if #[cfg(loom)] {
                        yield_now();
                    } else {
                        spin_loop();
                    }
                };
                continue;
            }
            if self
                .read_idx
                .compare_exchange_weak(idx, new_idx, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                cfg_if::cfg_if! {
                    if #[cfg(loom)] {
                        yield_now();
                    } else {
                        spin_loop();
                    }
                };
                continue;
            };
            // SAFETY:
            // - index is unique given the time checked
            // - last operation is memory uninitialization, safe for writes
            // - does not re-enter on a full loop; uninitialization is specific and can't read uninitialized data
            cfg_if::cfg_if! {
                if #[cfg(loom)] {
                    unsafe {
                        rr = self.buffer.get_unchecked(i).data.with(|ptr| core::ptr::read(ptr).assume_init());
                        self.buffer.get_unchecked(i)
                            .stamp
                            .store((idx + 1) ^ *self.mcb, Ordering::Release);
                    }
                } else {
                    unsafe {
                        rr = self.buffer.get_unchecked(i).data.get().read().assume_init();
                        self.buffer.get_unchecked(i)
                            .stamp
                            .store((idx + 1) ^ *self.mcb, Ordering::Release);
                    }
                }
            };
            limitless_probes::read__done!(|| (&probe_id, i as u64, 1u8));
            return Ok(rr);
        }
    }

    pub fn write(&self, v: T) -> Result<(), RingBufferError> {
        let probe_id = usdt::UniqueId::new();
        limitless_probes::write__start!(|| &probe_id);
        loop {
            let idx = self.write_idx.load(Ordering::Acquire);
            let i = idx & (*self.mcb - 1);
            let at_capacity = (i + 1 >= *self.capacity) as usize;
            let new_idx = ((idx + 1) & at_capacity.wrapping_sub(1))
                | (at_capacity * ((idx & *self.mcb) ^ *self.mcb));
            if self.buffer[i].stamp.load(Ordering::Acquire) != idx + 1 {
                let ridx = self.read_idx.load(Ordering::Acquire);
                if self.full(ridx, idx) {
                    limitless_probes::write__done!(|| (&probe_id, i as u64, 0u8));
                    return Err(RingBufferError::Full);
                }
                // spin until uninitialized
                cfg_if::cfg_if! {
                    if #[cfg(loom)] {
                        yield_now();
                    } else {
                        spin_loop();
                    }
                };
                continue;
            }
            if self
                .write_idx
                .compare_exchange_weak(idx, new_idx, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                // Note: maybe need to add jitter/backoff here
                cfg_if::cfg_if! {
                    if #[cfg(loom)] {
                        yield_now();
                    } else {
                        spin_loop();
                    }
                };
                continue;
            };
            // SAFETY:
            // - index is unique given the time checked, safe for writes
            // - last operation is memory initialization, safe for reads
            // - does not re-enter on a full loop; initialization is not ambiguous, no overwrite
            cfg_if::cfg_if! {
                if #[cfg(loom)] {
                    unsafe {
                        self.buffer
                            .get_unchecked(i)
                            .data
                            .with_mut(|ptr| core::ptr::write(ptr, MaybeUninit::new(v)));
                        self.buffer.get_unchecked(i).stamp.store(idx, Ordering::Release);
                    }
                } else {
                    unsafe {
                        self.buffer.get_unchecked(i).data.get().write(MaybeUninit::new(v));
                        self.buffer.get_unchecked(i).stamp.store(idx, Ordering::Release);
                    }
                }
            };
            limitless_probes::write__done!(|| (&probe_id, i as u64, 1u8));
            return Ok(());
        }
    }
}

// ****************************************
//           monomorphic wrappers
// ****************************************
#[doc(hidden)]
#[inline(never)]
pub fn __instantiate_ringbuffer_usize() -> RingBuffer<usize> {
    RingBuffer::new(20)
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::collections::HashMap;
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

        // empty
        assert_eq!(Err(RingBufferError::Empty), rb.read());

        // cycle twice to catch syncronization issues between empty/full state changes
        for cnt in 0..2 {
            println!("iteration {cnt}");
            for i in 0..(SIZE) {
                println!("write {i}");
                assert_eq!(Ok(()), rb.write(i));
            }
            // full
            assert_eq!(Err(RingBufferError::Full), rb.write(6));
            for i in 0..(SIZE) {
                assert_eq!(Ok(i), rb.read());
            }
            // empty
            assert_eq!(Err(RingBufferError::Empty), rb.read());
        }
    }

    #[test]
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
    fn test_multi_threaded_not_loom() {
        use std::collections::HashSet;
        const SIZE: usize = 3;
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
        assert!(hs_1.is_disjoint(&hs_2) || hs_1.is_empty());
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use std::collections::HashMap;
    use {loom::sync::Arc, loom::thread};

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
    fn test_two_threads_non_copy_type_rw() {
        let mut mdl = loom::model::Builder::new();
        mdl.preemption_bound = Some(2);
        mdl.check(|| {
            const SIZE: usize = 2;
            let rb: Arc<RingBuffer<DataCorruptor>> = Arc::new(RingBuffer::new(SIZE));
            let rbc = rb.clone();

            let t1 = thread::spawn(move || {
                for _ in 0..SIZE {
                    let _ = rbc.read();
                }
            });

            for i in 0..SIZE {
                let mut hm = HashMap::new();
                hm.insert("hello".to_owned(), vec![i, i + 1, i + 2]);
                let w = match i {
                    n if n % 2 == 0 => WishyWashy::Yes,
                    n if n % 3 == 0 => WishyWashy::No,
                    _ => WishyWashy::Maybe,
                };
                let _ = rb.write(DataCorruptor { val1: hm, val2: w });
            }

            let _ = t1.join().unwrap();
        });
    }

    #[test]
    fn test_two_threads_non_copy_type_ww() {
        let mut mdl = loom::model::Builder::new();
        mdl.preemption_bound = Some(2);
        mdl.check(|| {
            const SIZE: usize = 2;
            let rb: Arc<RingBuffer<DataCorruptor>> = Arc::new(RingBuffer::new(SIZE));
            let rbc = rb.clone();

            let t1 = thread::spawn(move || {
                for i in 0..SIZE {
                    let mut hm = HashMap::new();
                    hm.insert("hello".to_owned(), vec![i, i + 1, i + 2]);
                    let w = match i {
                        n if n % 2 == 0 => WishyWashy::Yes,
                        n if n % 3 == 0 => WishyWashy::No,
                        _ => WishyWashy::Maybe,
                    };
                    let _ = rbc.write(DataCorruptor { val1: hm, val2: w });
                }
            });

            for i in 0..SIZE {
                let mut hm = HashMap::new();
                hm.insert("hello".to_owned(), vec![i, i + 1, i + 2]);
                let w = match i {
                    n if n % 2 == 0 => WishyWashy::Yes,
                    n if n % 3 == 0 => WishyWashy::No,
                    _ => WishyWashy::Maybe,
                };
                let _ = rb.write(DataCorruptor { val1: hm, val2: w });
            }

            let _ = t1.join().unwrap();
        });
    }

    #[test]
    fn test_two_threads_non_copy_type_rr() {
        let mut mdl = loom::model::Builder::new();
        mdl.preemption_bound = Some(2);
        mdl.check(|| {
            const SIZE: usize = 2;
            let rb: Arc<RingBuffer<DataCorruptor>> = Arc::new(RingBuffer::new(SIZE));
            let rbc = rb.clone();

            for i in 0..SIZE {
                let mut hm = HashMap::new();
                hm.insert("hello".to_owned(), vec![i, i + 1, i + 2]);
                let w = match i {
                    n if n % 2 == 0 => WishyWashy::Yes,
                    n if n % 3 == 0 => WishyWashy::No,
                    _ => WishyWashy::Maybe,
                };
                let _ = rb.write(DataCorruptor { val1: hm, val2: w });
            }

            let t1 = thread::spawn(move || {
                for _ in 0..SIZE {
                    let _ = rbc.read();
                }
            });

            let _ = rb.read();
            let _ = t1.join().unwrap();
        });
    }
}
