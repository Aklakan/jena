package org.apache.jena.sparql.service.enhancer.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public abstract class LockWrapper
    implements Lock
{
    protected abstract Lock getDelegate();

    @Override
    public void lock() {
        getDelegate().lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        getDelegate().lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return getDelegate().tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return getDelegate().tryLock();
    }

    @Override
    public void unlock() {
        getDelegate().unlock();
    }

    @Override
    public Condition newCondition() {
        return getDelegate().newCondition();
    }
}
