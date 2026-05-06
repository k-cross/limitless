use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Implementation of a lock-free ring buffer that takes a fixed and unchangable
/// size of items to store. Items must implement the Default trait in order to
/// be used.
#[derive(Debug)]
pub struct RingBuffer<T, const N: usize> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>; N]>,
    capacity: usize,
    size: AtomicUsize,
    read_idx: AtomicUsize,
    write_idx: AtomicUsize,
}

unsafe impl<T, const N: usize> Send for RingBuffer<T, N> {}
unsafe impl<T, const N: usize> Sync for RingBuffer<T, N> {}

impl<T, const N: usize> RingBuffer<T, N> {
    pub fn new() -> Self {
        Self {
            buffer: Box::new(std::array::from_fn(|_| {
                UnsafeCell::new(MaybeUninit::<T>::uninit())
            })),
            capacity: N,
            size: AtomicUsize::new(0),
            read_idx: AtomicUsize::new(0),
            write_idx: AtomicUsize::new(0),
        }
    }

    pub fn is_full(&self) -> bool {
        self.size.load(Ordering::Acquire) == self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.size.load(Ordering::Acquire) == 0
    }

    pub fn read(&self) -> Result<T, ()> {
        if self.is_empty() {
            return Err(());
        }
        let idx = self.read_idx.load(Ordering::Acquire);
        let r = self.buffer[idx].get();
        if let Err(_) = self.read_idx.compare_exchange(
            idx,
            (idx + 1) % self.capacity,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            return self.read();
        };
        self.size.fetch_sub(1, Ordering::AcqRel);

        unsafe { Ok(r.read().assume_init()) }
    }

    pub fn write(&self, v: T) -> Result<(), ()> {
        if self.is_full() {
            return Err(());
        }
        let idx = self.write_idx.load(Ordering::Acquire);
        if let Err(_) = self.write_idx.compare_exchange(
            idx,
            (idx + 1) % self.capacity,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            return self.write(v);
        };
        // maybe need to do a drop(old_ptr), need to verify memory doesn't leak
        unsafe { self.buffer[idx].get().write(MaybeUninit::new(v)) };
        self.size.fetch_add(1, Ordering::AcqRel);
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
        let rb: RingBuffer<usize, 5> = RingBuffer::new();
        for i in 0..5 {
            assert_eq!(Ok(()), rb.write(i));
        }
        assert_eq!(Err(()), rb.write(6));
        assert_eq!(rb.size.load(Ordering::Acquire), 5);
        for i in 0..5 {
            assert_eq!(Ok(i), rb.read());
        }
        assert_eq!(Err(()), rb.read());
        assert_eq!(rb.size.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_multi_threaded_independent_read_and_write() {
        const SIZE: usize = 1024 * 16;
        let logical_cores: usize = thread::available_parallelism().unwrap().into();
        // needs to be even for test assumptions to work
        //let iteration_size = logical_cores * 2;
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
                    for _ in 0..SIZE {
                        // complete regardless of contention
                        if let Err(_) = rbc.write(1) {
                            se.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            } else {
                thread::spawn(move || {
                    // always empty the ring buffer but it might be better to
                    // move this to the main thread
                    while !rbc.is_empty() {
                        if let Ok(r) = rbc.read() {
                            s.fetch_add(r, Ordering::SeqCst);
                        }
                    }
                })
            };
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        let result = (SIZE * iteration_size / 2) - sum_err.load(Ordering::Acquire);
        println!("result is {result}");
        assert_eq!(sum.load(Ordering::Acquire), result);
    }
}
