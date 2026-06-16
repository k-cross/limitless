# Limitless

A project that implements high-performance thread safe data structures.

# Performance Analysis

Given my machines are usually MacOS I did performance analysis using it unless otherwise stated.

Under a high contention scenario with 4 read and 4 write threads, the following was observed:
- L1 Cache Miss Rate: 31%
- Instructions per Clock: 0.45
- Full CPU utilization: N - 1 logical cores
