use limitless::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

fn main() {
    const SIZE: usize = 1024 * 16;
    let logical_cores: usize = thread::available_parallelism().unwrap().into();
    // needs to be EVEN for test assumptions to work
    let iteration_size = if logical_cores % 2 == 0 {
        logical_cores
    } else {
        logical_cores + 1
    };
    let mut loop_counter: usize = 0;
    loop {
        println!("loop {loop_counter}");
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
                        if let Err(_) = rbc.write(2) {
                            se.fetch_add(1, Ordering::AcqRel);
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
                            assert_eq!(2, r);
                            s.fetch_add(r, Ordering::AcqRel);
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
                assert_eq!(2, r);
                sum.fetch_add(r, Ordering::AcqRel);
            }
        }
        let result = (2 * SIZE * iteration_size / 2) - (2 * sum_err.load(Ordering::Acquire));
        println!("result is {result}");
        assert_eq!(sum.load(Ordering::Acquire), result);
        loop_counter += 1;
    }
}
