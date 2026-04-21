use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Implementation of a lock-free ring buffer that takes a fixed and unchangable
/// size of items to store. Items must implement the Default trait in order to
/// be used.
#[derive(Debug)]
pub struct RingBuffer<T, const N: usize> {
    buffer: [AtomicPtr<T>; N],
    capacity: usize,
    size: AtomicUsize,
    read_idx: AtomicUsize,
    write_idx: AtomicUsize,
}

unsafe impl<T, const N: usize> Send for RingBuffer<T, N> {}
unsafe impl<T, const N: usize> Sync for RingBuffer<T, N> {}

impl<T: Default, const N: usize> RingBuffer<T, N> {
    pub fn new() -> Self {
        Self {
            buffer: std::array::from_fn(|_| {
                let b = Box::into_raw(Box::new(T::default()));
                AtomicPtr::new(b)
            }),
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

    pub fn read(&mut self) -> Result<T, ()> {
        if self.is_empty() {
            return Err(());
        }
        let idx = self.read_idx.load(Ordering::Acquire);
        let r = self.buffer[idx].load(Ordering::Acquire);
        if let Err(_) = self.read_idx.compare_exchange(
            idx,
            (idx + 1) % self.capacity,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            return self.read();
        };
        self.size.fetch_sub(1, Ordering::AcqRel);

        unsafe { Ok(r.read()) }
    }

    pub fn write(&mut self, v: T) -> Result<(), ()> {
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
        self.buffer[idx].swap(Box::into_raw(Box::new(v)), Ordering::SeqCst);
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
        let mut rb: RingBuffer<usize, 5> = RingBuffer::new();
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
        let rb: Arc<RingBuffer<usize, SIZE>> = Arc::new(RingBuffer::new());
        for i in 0..10 {
            let rbc = rb.clone();
            if i % 2 == 0 {
                thread::spawn(move || rbc.write(1));
            } else {
                thread::spawn(move || rbc.read());
            }
        }
    }
}
