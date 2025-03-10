package org.apache.jena.sparql.service.enhancer.concurrent;

import java.util.concurrent.locks.Lock;

public class AutoLock implements AutoCloseable {
    private final Lock lock;

    private AutoLock(Lock lock) {
        this.lock = lock;
    }

    /**
     * Immediately attempts to acquire the lock and returns
     * an auto-closeable AutoLock instance for use with try-with-resources.
     */
    public static AutoLock lock(Lock lock) {
        lock.lock();
        return new AutoLock(lock);
    }

    @Override
    public void close() {
        lock.unlock();
    }
}
