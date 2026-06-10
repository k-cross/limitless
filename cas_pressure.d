/* Count CAS failures as a proxy for contention */
pid$target::OSAtomicCompareAndSwap*:entry
{
    @cas_attempts[tid] = count();
}

pid$target::OSAtomicCompareAndSwap*:return
/arg1 == 0/   /* return value 0 = failure */
{
    @cas_failures[tid] = count();
}
