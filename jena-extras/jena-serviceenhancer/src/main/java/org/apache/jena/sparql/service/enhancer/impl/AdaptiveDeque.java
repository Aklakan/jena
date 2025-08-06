package org.apache.jena.sparql.service.enhancer.impl;

import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ForwardingDeque;

/**
 * Dequeue wrapper whose capacity can be updated dynamically.
 */
public class AdaptiveDeque<E> extends ForwardingDeque<E> {
    private final Deque<E> delegate;
    private final AtomicInteger capacity;

    public AdaptiveDeque(Deque<E> delegate) {
    	this(Integer.MAX_VALUE, delegate);
    }

    public AdaptiveDeque(int initialCapacity, Deque<E> delegate) {
        this.delegate = delegate;
        this.capacity = new AtomicInteger(initialCapacity);
    }

    @Override
    protected Deque<E> delegate() {
        return delegate;
    }

    public void setCapacity(int newCapacity) {
        capacity.set(newCapacity);
    }

    @Override
    public boolean offer(E e) {
        if (size() >= capacity.get()) {
            return false;
        }
        return super.offer(e);
    }

    @Override
    public void addFirst(E e) {
        if (size() >= capacity.get()) {
            throw new IllegalStateException("Deque is at dynamic capacity");
        }
        super.addFirst(e);
    }

    @Override
    public void addLast(E e) {
        if (size() >= capacity.get()) {
            throw new IllegalStateException("Deque is at dynamic capacity");
        }
        super.addLast(e);
    }

    @Override
	public boolean addAll(Collection<? extends E> collection) {
        if (size() + collection.size() >= capacity.get()) {
            throw new IllegalStateException("Deque is at dynamic capacity");
        }
        return super.addAll(collection);

        // Alternative:
        // return standardAddAll(collection);
	}
}
