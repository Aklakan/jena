package org.apache.jena.sparql.service.enhancer.concurrent;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class ReadWriteLockModular
    implements ReadWriteLock
{
    protected Lock readLock;
    protected Lock writeLock;

    public ReadWriteLockModular(Lock readLock, Lock writeLock) {
        super();
        this.readLock = Objects.requireNonNull(readLock);
        this.writeLock = Objects.requireNonNull(writeLock);
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }
}
