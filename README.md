# Limitless

A project that implements high-performance thread safe data structures.

# Performance Analysis

Given my machines are usually MacOS I did quite a bit of performance analysis using MacOS so unless otherwise stated, that should be the operating assumption.

There is a `D` script which measures the CAS pressure using DTrace on MacOS:

- run with `sudo dtrace -s cas.d -c target/release/limitless`
