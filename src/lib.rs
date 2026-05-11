use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Slot<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    initialized: AtomicBool,
}

/// Implementation of a lock-free ring buffer that takes a fixed and unchangable
/// size of items to store.
#[derive(Debug)]
pub struct RingBuffer<T, const N: usize> {
    buffer: Box<[Slot<T>; N]>,
    capacity: usize,
    read_idx: AtomicUsize,
    write_idx: AtomicUsize,
}

unsafe impl<T, const N: usize> Send for RingBuffer<T, N> {}
unsafe impl<T, const N: usize> Sync for RingBuffer<T, N> {}

impl<T, const N: usize> RingBuffer<T, N> {
    pub fn new() -> Self {
        Self {
            buffer: Box::new(std::array::from_fn(|_| Slot {
                data: UnsafeCell::new(MaybeUninit::uninit()),
                initialized: AtomicBool::new(false),
            })),
            capacity: N,
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
            if self.is_empty() {
                return Err(());
            }
            let idx = self.read_idx.load(Ordering::Acquire);
            if !self.buffer[idx].initialized.load(Ordering::Acquire) {
                continue;
            }
            let r = self.buffer[idx].data.get();
            if let Err(_) = self.read_idx.compare_exchange_weak(
                idx,
                (idx + 1) % self.capacity,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                continue;
            };
            // Note: maybe need to do a drop(old_ptr), need to verify memory doesn't leak
            rr = unsafe { r.read().assume_init() };
            self.buffer[idx].initialized.store(false, Ordering::Release);
            break;
        }
        Ok(rr)
    }

    pub fn write(&self, v: T) -> Result<(), ()> {
        loop {
            if self.is_full() {
                return Err(());
            }
            let idx = self.write_idx.load(Ordering::Acquire);
            if self.buffer[idx].initialized.load(Ordering::Acquire) {
                continue;
            }
            if let Err(_) = self.write_idx.compare_exchange_weak(
                idx,
                (idx + 1) % self.capacity,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                // Note: maybe need to add jitter/backoff here
                continue;
            };
            // Note: maybe need to do a drop(old_ptr), need to verify memory doesn't leak
            unsafe { self.buffer[idx].data.get().write(MaybeUninit::new(v)) };
            self.buffer[idx].initialized.store(true, Ordering::Release);
            break;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_single_threaded() {
        // Sequential Access
        const SIZE: usize = 5;
        let rb: RingBuffer<usize, SIZE> = RingBuffer::new();
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
        let rb: Arc<RingBuffer<usize, SIZE>> = Arc::new(RingBuffer::new());
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
        let result = (SIZE * iteration_size / 2) - sum_err.load(Ordering::Acquire);
        println!("result is {result}");
        assert_eq!(sum.load(Ordering::SeqCst), result);
    }
}
