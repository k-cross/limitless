# Limitless

A project that implements high-performance thread safe data structures.

# Performance Analysis

Given my machines are usually MacOS I did performance analysis using it unless otherwise stated.

Under a high contention scenario with 4 read and 4 write threads, the following was observed:
- L1 Cache Miss Rate: 17%
- Instructions per Clock: 0.08
- Atomic Miss Rate: 53%
- Full CPU utilization: N - 1 logical cores

Under the same benchmark, it performs twice as fast as the `ArrayQueue`
provided by _crossbeam_, but this is not really intended to be a replacement
for it either.

## Tail Latency

USDT probes are included and can be measured using the `dtrace` script:

```sh
sudo dtrace -c ./target/release/limitless -s ./tail_latency.d
```

For Linux, you'll have to use `perf` or an `ebpf` program to perform the analysis but it's possible although I don't provide the script.

# References

- Project [implementation](https://k-cross.github.io/limits1/) details
