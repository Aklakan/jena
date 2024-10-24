package org.apache.jena.sparql.service.enhancer.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Class which consumes items from an iterator upon calling {@link #run()} until {@link #stop()} is called. */
public class PrefetchTask<T, I extends Iterator<T>>
    implements Runnable
{
    public enum State {
        CREATED,
        STARTING,
        RUNNING,
        TERMINATED
    }

    protected volatile I iterator;
    protected volatile List<T> bufferedItems;
    protected long maxBufferedItemsCount;

    protected volatile boolean isStopRequested;

    protected volatile State state = State.CREATED;

    public PrefetchTask(I iterator, long maxBufferedItemsCount) {
        this(iterator, new ArrayList<>(1024), maxBufferedItemsCount);
    }

    public PrefetchTask(I iterator, List<T> bufferedItems, long maxBufferedItemsCount) {
        super();
        this.maxBufferedItemsCount = maxBufferedItemsCount;
        this.iterator = iterator;
        this.bufferedItems = bufferedItems;
    }

    public List<T> getBufferedItems() {
        return bufferedItems;
    }

    public I getIterator() {
        return iterator;
    }

    public State getState() {
        return state;
    }

    @Override
    public void run() {
        state = State.STARTING;
        while (!isStopRequested && !Thread.interrupted() && iterator.hasNext() && bufferedItems.size() < maxBufferedItemsCount) {
            state = State.RUNNING;
            T item = iterator.next();
            bufferedItems.add(item);
        }
        state = State.TERMINATED;
    }

    public void stop() {
        isStopRequested = true;
    }

    public static <T, I extends Iterator<T>> PrefetchTask<T, I> of(I iterator, long maxBufferedItemsCount) {
        return new PrefetchTask<>(iterator, maxBufferedItemsCount);
    }
}
