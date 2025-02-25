/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.service.enhancer.concurrent.ExecutorServicePool;
import org.apache.jena.sparql.service.enhancer.impl.util.InstanceLifeCycle;
import org.apache.jena.sparql.service.enhancer.impl.util.InstanceLifeCycles;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterator;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIteratorConcat;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIteratorPeek;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterators;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbstractAbortableIterator;

import com.google.common.io.Closer;

/** A flat map iterator that can read ahead in the input iterator and run flat map operations concurrently. */
public abstract class RequestExecutorBase<G, I, O>
    extends AbstractAbortableIterator<O>
{
    /** Whether to order output across all batches by the individual items (ITEM) or whether each batch is processed as one consecutive unit (BATCH). */
    public enum Granularity {
        ITEM,
        BATCH
    }

    /**
     * Interface for the customizing iterator creation.
     * Each method is only invoked once and all methods are always invoked on the same thread.
     * The end method will be invoked upon closing the iterator or if iterator creation fails.
     *
     */
    public interface IteratorCreator<T> {
        default void begin() {}
        AbortableIterator<T> createIterator();
        default void end() {}
    }

    static class IteratorDelegateWithWorkerThread<T, X extends Iterator<T>>
        extends AbstractAbortableIterator<T>
    {
        protected ExecutorServiceWrapperSync executorServiceSync;

        /** Number of items to transfer in batch from the worker thread to the calling thread */
        protected int batchSize;
        protected X delegate;

        protected volatile List<T> buffer;
        protected Iterator<T> currentBatch;

        /** Certain objects such as TDB2 Bindings must be copied in order to detach them from
         * resources that are free'd when the iterator is closed.
         */
        protected UnaryOperator<T> copyFn;

        public IteratorDelegateWithWorkerThread(X delegate, ExecutorService es, UnaryOperator<T> copyFn) {
            this(delegate, es, copyFn, 1);
        }

        public IteratorDelegateWithWorkerThread(X delegate, ExecutorService es, UnaryOperator<T> copyFn, int batchSize) {
            super();
            this.executorServiceSync = new ExecutorServiceWrapperSync(es);
            this.delegate = delegate;
            this.copyFn = copyFn;
            this.batchSize = batchSize;

            this.buffer = new ArrayList<>(batchSize);
            this.currentBatch = buffer.iterator();
        }

        public X getDelegate() {
            return delegate;
        }

        @Override
        protected T moveToNext() {
            T result;
            if (currentBatch.hasNext()) {
                result = currentBatch.next();
            } else {
                buffer.clear();
                executorServiceSync.submit(() -> {
                    X d = getDelegate();
                    for (int i = 0; i < batchSize && d.hasNext(); ++i) {
                        T rawItem = d.next();
                        T item = copyFn.apply(rawItem);
                        buffer.add(item);
                    }
                });
                currentBatch = buffer.iterator();

                result = currentBatch.hasNext()
                    ? currentBatch.next()
                    : endOfData();
            }
            return result;
        }

        /**
         * Close the iterator.
         * Note that the worker is blocked while retrieving so in that case any close signal won't get through.
         */
        @Override
        public final void closeIteratorActual() {
            try {
                executorServiceSync.submit(this::inThreadCloseAction);
            } finally {
                outThreadCloseAction();
            }
        }

        @Override
        protected void requestCancel() {}

        /** Close action that is submitted to the executor service. */
        protected void inThreadCloseAction() {
            Iter.close(getDelegate());
        }

        /** Close action that is run by the calling thread. */
        protected void outThreadCloseAction() {}

        @Override
        public void output(IndentedWriter out, SerializationContext sCxt) {
            // FIXME Auto-generated method stub
        }
    }

    /** A prefetch task with metadata about which batch id and corresponding input ids it serves. */
    static class PrefetchTaskForBatch<T, X extends AbortableIterator<T>>
        extends PrefetchTaskBase<T, X> {

        protected long batchId;

        /** The input ids in ascending order served by this task. Never empty. */
        protected List<Long> servedInputIds;

        public PrefetchTaskForBatch(X iterator, long maxBufferedItemsCount, long batchId, List<Long> servedInputIds, UnaryOperator<T> copyFn) {
            super(iterator, maxBufferedItemsCount, copyFn);
            this.batchId = batchId;
            this.servedInputIds = Objects.requireNonNull(servedInputIds);

            if (servedInputIds.isEmpty()) {
                throw new IllegalArgumentException("Input ids must be neither null nor empty");
            }
        }

        long getBatchId() {
            return batchId;
        }

        public List<Long> getServedInputIds() {
            return servedInputIds;
        }

        @Override
        public String toString() {
            return "TaskId for batchId " + batchId + " with inputIds " + servedInputIds + " [" + state + (isStopRequested ? "aborted" : "") + "]: " + bufferedItems.size() + " items buffered.";
        }

        public static <T> PrefetchTaskForBatch<T, AbortableIterator<T>> empty(long closeInputId) {
            return new PrefetchTaskForBatch<>(AbortableIterators.empty(), 0, closeInputId, List.of(closeInputId), UnaryOperator.identity());
        }
    }

    protected static long getCloseId(Granularity granularity, PrefetchTaskForBatch<?, ?> task) {
        long result = switch (granularity) {
            case ITEM -> task.getServedInputIds().get(task.getServedInputIds().size() - 1);
            case BATCH -> task.getBatchId();
        };
        return result;
    }

    /** Helper record for tracking running prefetch tasks. */
    static class TaskEntry<T, X extends AbortableIterator<T>> {
        // protected LifeCycle<PrefetchTaskForBinding> taskLifeCycle;
        protected PrefetchTaskForBatch<T, X> task;
        protected Closeable inThreadCloseAction;

        protected ExecutorService executorService;
        protected Closeable outThreadCloseAction;

        protected Future<?> future;
        // protected ExecutionContext execCxt;

        protected volatile AbortableIteratorPeek<T> peekIter;

        public TaskEntry(PrefetchTaskForBatch<T, X> task, Closeable inThreadCloseAction, ExecutorService executorService, Closeable outThreadCloseAction, Future<?> future) {
            super();
            this.task = task;
            this.inThreadCloseAction = inThreadCloseAction;
            this.executorService = executorService;
            this.outThreadCloseAction = outThreadCloseAction;
            this.future = future;
        }

        public PrefetchTaskForBatch<T, X> task() {
            return task;
        }

        public Future<?> future() {
            return future;
        }

        /**
         * Stop the task and return an iterator over the buffered items and the remaining ones.
         * Closing the returned iterator also closes the iterator over the remaining items.
         *
         * In case of an exception, this task entry closes itself.
         */
        public AbortableIteratorPeek<T> stopAndGet() {
            if (peekIter == null) {
                // Send the abort signal
                task.stop();

                // Wait for the task to complete
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Tasks should never fail but instead set task.getThrowable()
                    throw new RuntimeException("Should not happen", e);
                }

                AbortableIterator<T> tmp = task.getIterator();

                // If there is an executorService then make sure the iterator is accessed using it.
                if (executorService != null) {
                    IteratorCloseable<T> threadIt = new IteratorDelegateWithWorkerThread<>(tmp, executorService, task.getCopyFn()) {
                        /** The close action runs inside of the executor service. */
                        @Override
                        protected void inThreadCloseAction() {
                            try {
                                super.inThreadCloseAction();
                            } finally {
                                if (inThreadCloseAction != null) {
                                    inThreadCloseAction.close();
                                }
                            }
                        }

                        @Override
                        protected void outThreadCloseAction() {
                            if (outThreadCloseAction != null) {
                                outThreadCloseAction.close();
                            }
                        }
                    };
                    // tmp = QueryIterPlainWrapper.create(threadIt, execCxt);
                    tmp = AbortableIterators.wrap(threadIt);
                } else {
                    tmp = AbortableIterators.onClose(tmp, inThreadCloseAction);
                }

                // If there are buffered items then prepend them to 'tmp'
                List<T> bufferedItems = task.getBufferedItems();
                if (!bufferedItems.isEmpty()) {
                    @SuppressWarnings("resource") // 'concat' will eventually be closed by the calling thread
                    AbortableIteratorConcat<T> concat = new AbortableIteratorConcat<>();
                    concat.add(AbortableIterators.wrap(bufferedItems.iterator()));
                    concat.add(tmp);
                    tmp = concat;
                }
                peekIter = new AbortableIteratorPeek<>(tmp);
            }

            Throwable throwable = task.getThrowable();
            if (throwable != null) {
                peekIter.close();
                // throwable.addSuppressed(new RuntimeException());
                // Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }

            return peekIter;
        }

        public static <T, X extends AbortableIterator<T>> TaskEntry<T, X> completed(PrefetchTaskForBatch<T, X> task, Closeable closeAction) {
            return new TaskEntry<>(
                task,
                closeAction,
                null,
                null,
                CompletableFuture.completedFuture(null)
            );
        }
        public static <T> TaskEntry<T, AbortableIterator<T>> empty(long closeInputId) {
            return completed(PrefetchTaskForBatch.empty(closeInputId), null);
        }
    }

    /**  Ensure that at least there are active requests to serve the next n input bindings */
    protected int fetchAhead = 5;
    protected int maxRequestSize = 2000;

    /** If batch granularity is true then output is ordered according to the obtained batches.
     *  Otherwise, output is ordered according to the individual member ids of the batches (assumes ids are unique across batches). */
    protected Granularity granularity;
    protected AbortableIterator<GroupedBatch<G, Long, I>> batchIterator;
    protected long nextBatchId = 0; // Counter of items taken from batchIterator

    // Input iteration
    protected long currentInputId = -1;
    protected TaskEntry<O, AbortableIterator<O>> activeTaskEntry = null;
    protected AbortableIteratorPeek<O> activeIter;

    /** Tasks ordered by their input id - regardless of whether they are run concurrently or not. */
    protected Map<Long, TaskEntry<O, AbortableIterator<O>>> inputToOutputIt = new LinkedHashMap<>();

    /* State for tracking concurrent prefetch ----------------------------- */

    // private ExecutorService executorService = null;
    protected ExecutorServicePool executorServicePool;

    private final int maxConcurrentTasks;
    private final long concurrentSlotReadAheadCount;

    /** The concurrently running tasks. */
    private final Map<Long, TaskEntry<O, AbortableIterator<O>>> openConcurrentTaskEntries = new LinkedHashMap<>();

    /* Actual implementation ---------------------------------------------- */

    public RequestExecutorBase(
            AtomicBoolean cancelSignal,
            Granularity granularity,
            AbortableIterator<GroupedBatch<G, Long, I>> batchIterator,
            int maxConcurrentTasks,
            long concurrentSlotReadAheadCount) {
        super(cancelSignal);
        this.granularity = Objects.requireNonNull(granularity);
        this.batchIterator = Objects.requireNonNull(batchIterator);

        // Set up a dummy task with an empty iterator as the active one and ensure it is properly closed
        activeTaskEntry = TaskEntry.empty(currentInputId);
        inputToOutputIt.put(currentInputId, activeTaskEntry);

        this.activeIter = activeTaskEntry.stopAndGet();
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.concurrentSlotReadAheadCount = concurrentSlotReadAheadCount;

        this.executorServicePool = new ExecutorServicePool();
    }

    @Override
    protected O moveToNext() {
        O result = null;

        // Peek the next binding on the active iterator and verify that it maps to the current
        // partition key
        // while (!isCancelled()) {
        while (true) {
            if (activeIter.hasNext()) {
                O peek = activeIter.peek();
                // The iterator returns null if it was aborted
                if (peek == null) {
                    break;
                }

                // On batch granularity always take the lowest input id served by a task
                long peekOutputId = switch(granularity) {
                    case ITEM -> extractInputOrdinal(peek);
                    case BATCH -> activeTaskEntry.task().getBatchId();
                };

                // long peekOutputId = BindingUtils.getNumber(peek, globalIdxVar).longValue();

                boolean inputIdMatches = peekOutputId == currentInputId;
                if (inputIdMatches) {
                    // parentBinding = inputToBinding.get(currentInputId);
                    result = activeIter.next();
                    break;
                }
            }

            // Cleanup of no longer needed resources
            long closeId = getCloseId(granularity, activeTaskEntry.task());
            boolean isClosePoint = currentInputId == closeId;
            if (isClosePoint) {
                activeIter.close();
                inputToOutputIt.remove(currentInputId);
                openConcurrentTaskEntries.remove(currentInputId);
            }

            // Move to the next inputId
            ++currentInputId;

            // Check if we need to load any further batches
            prepareNextBatchExecs();

            // If there is still no further batch then we assume we reached the end
            activeTaskEntry = inputToOutputIt.get(currentInputId);
            if (activeTaskEntry == null) {
                break;
            }

            activeIter = activeTaskEntry.stopAndGet();
        }

        if (result == null) {
            result = endOfData();
        }

        return result;
    }

    protected void registerTaskEntry(TaskEntry<O, AbortableIterator<O>> taskEntry) {
        switch (granularity) {
        case ITEM:
            List<Long> servedInputIds = taskEntry.task().getServedInputIds();
            for (Long e : servedInputIds) {
                inputToOutputIt.put(e, taskEntry);
            }
            break;
        case BATCH:
            long batchId = taskEntry.task().getBatchId();
            inputToOutputIt.put(batchId, taskEntry);
            break;
        default:
            throw new IllegalStateException("Should never come here.");
        }
    }

    public void prepareNextBatchExecs() {
        if (!inputToOutputIt.containsKey(currentInputId)) {
            // We need the task's iterator right away - do not start concurrent retrieval
            if (batchIterator.hasNext()) {
                InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(false);
                PrefetchTaskForBatch<O, AbortableIterator<O>> task = taskLifeCycle.newInstance();
                TaskEntry<O, AbortableIterator<O>> taskEntry = TaskEntry.completed(task, () -> taskLifeCycle.closeInstance(task));
                registerTaskEntry(taskEntry);
            }
        }

        // Give the implementation a chance to reject concurrent execution via an exception.
        // For example, if a READ_PROMOTE transaction switched to WRITE than concurrent READ might cause a deadlock.
        checkCanExecInNewThread();

        // Fill any remaining slots in the task queue for concurrent processing
        // Concurrent tasks have their own execution contexts because execCxt is not thread safe.
        while (openConcurrentTaskEntries.size() < maxConcurrentTasks && batchIterator.hasNext() && !isCancelled()) {

            // InstanceLifeCycle<PrefetchTaskForBinding> taskLifeCycle = prepareNextBatchExec(isolatedExecCxt, txnType);
            InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(true);
            ExecutorService executorService = executorServicePool.acquireExecutor(); //.acquireExecutor(); // Executors.newSingleThreadExecutor();

            // Create the task through the execution thread such that thread locals (e.g. for transactions)
            // are initialized on the correct thread
            PrefetchTaskForBatch<O, AbortableIterator<O>> task = ExecutorServiceWrapperSync.submit(executorService, taskLifeCycle::newInstance);

            // Submit the task which immediately starts concurrent retrieval.
            // Retrieval happens on the same thread on which the task was created.
            Future<?> future = executorService.submit(task);
            TaskEntry<O, AbortableIterator<O>> taskEntry = new TaskEntry<>(
                    task,
                    () -> taskLifeCycle.closeInstance(task),
                    executorService,
                    () -> executorService.shutdown(), // This action returns the acquired executor service back to the pool
                    future);

            registerTaskEntry(taskEntry);
            long closeId = getCloseId(granularity, task);
            openConcurrentTaskEntries.put(closeId, taskEntry);
        }
    }

    protected abstract boolean isCancelled();

    protected abstract IteratorCreator<O> processBatch(boolean isInNewThread, G groupKey, List<I> batch, List<Long> reverseMap);

    protected abstract long extractInputOrdinal(O input);
    protected abstract void checkCanExecInNewThread();

    protected O copy(O item) { return item; }

    /** Prepare the lazy execution of the next batch and register all iterators with {@link #inputToOutputIt} */
    // seqId = sequential number injected into the request
    // inputId = id (index) of the input binding
    // rangeId = id of the range w.r.t. to the input binding
    // partitionKey = (inputId, rangeId)
    protected InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> prepareNextBatchExec(boolean isInNewThread) {
        GroupedBatch<G, Long, I> batchRequest = batchIterator.next();
        long batchId = nextBatchId++;
        Batch<Long, I> batch = batchRequest.getBatch();
        NavigableMap<Long, I> batchItems = batch.getItems();

        G groupKey = batchRequest.getGroupKey();
        List<I> inputs = List.copyOf(batchItems.values());
        List<Long> reverseMap = List.copyOf(batchItems.keySet());

        IteratorCreator<O> creator = processBatch(isInNewThread, groupKey, inputs, reverseMap);

        InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> result = InstanceLifeCycles.of(() -> {
            creator.begin();
            AbortableIterator<O> tmp = creator.createIterator();
            PrefetchTaskForBatch<O, AbortableIterator<O>> task = new PrefetchTaskForBatch<>(tmp, concurrentSlotReadAheadCount, batchId, reverseMap, this::copy);
            return task;
        }, task -> {
            try {
                task.getIterator().close();
            } finally {
                creator.end();
            }
        });

        return result;
    }

    /** Check whether the values in the given collection are consecutive. */
    private static boolean isConsecutive(Collection<Long> ids) {
        boolean result = true;
        Iterator<Long> it = ids.iterator();
        if (it.hasNext()) {
            long start = it.next();
            long expected = start + 1;
            while (it.hasNext()) {
                long actual = it.next();
                if (actual != expected) {
                    // throw new RuntimeException("Concurrent processor must receive input ids in consecutive order. Adjust the batching strategy accordingly.");
                    result = false;
                    break;
                }
                ++expected;
            }
        }
        return result;
    }

    protected void freeResources() {
        // Use closer to free as much as possible in case of failure.
        try (Closer closer = Closer.create()) {
            closer.register(activeIter::close);

            for (TaskEntry<O, AbortableIterator<O>> taskEntry : inputToOutputIt.values()) {
                Closeable closable = taskEntry.stopAndGet();
                closer.register(closable::close);
            }

            closer.register(batchIterator::close);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void closeIteratorActual() {
        freeResources();
    }

    @Override
    protected void requestCancel() {
        batchIterator.cancel();
    }
}
