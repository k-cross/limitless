use criterion::{Criterion, criterion_group, criterion_main};
use limitless::RingBuffer;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

fn multi_threaded(iteration_size: usize) {
    const SIZE: usize = 1024 * 16;
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
                for _ in 0..SIZE {
                    // complete regardless of contention
                    if rbc.write(2).is_err() {
                        se.fetch_add(1, Ordering::AcqRel);
                    }
                }
            })
        } else {
            thread::spawn(move || {
                // always empty the ring buffer but it might be better to
                // move this to the main thread
                while !rbc.is_empty() {
                    if let Ok(r) = rbc.read() {
                        assert_eq!(2, r);
                        s.fetch_add(r, Ordering::AcqRel);
                    }
                }
            })
        };
        threads.push(t);
    }
    for t in threads {
        t.join().unwrap();
    }
    while !rb.is_empty() {
        if let Ok(r) = rb.read() {
            assert_eq!(2, r);
            sum.fetch_add(r, Ordering::AcqRel);
        }
    }
    let result = (2 * SIZE * iteration_size / 2) - (2 * sum_err.load(Ordering::Acquire));
    assert_eq!(sum.load(Ordering::Acquire), result);
}

fn criterion_benchmark(c: &mut Criterion) {
    let logical_cores: usize = thread::available_parallelism().unwrap().into();
    // needs to be EVEN for test assumptions to work
    let iteration_size = if logical_cores.is_multiple_of(2) {
        logical_cores
    } else {
        logical_cores + 1
    };
    c.bench_function("multi-threaded usize", |b| {
        b.iter(|| multi_threaded(black_box(iteration_size)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
