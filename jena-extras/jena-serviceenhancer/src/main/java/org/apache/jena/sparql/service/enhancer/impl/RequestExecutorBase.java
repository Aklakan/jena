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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIteratorBase;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIteratorConcat;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIteratorPeek;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterators;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbstractAbortableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

/** A flat map iterator that can read ahead in the input iterator and run flat map operations concurrently. */
public abstract class RequestExecutorBase<G, I, O>
    extends AbstractAbortableIterator<O>
{
    private static final Logger logger = LoggerFactory.getLogger(RequestExecutorBase.class);

    /** Whether to order output across all batches by the individual items (ITEM) or whether each batch is processed as one consecutive unit (BATCH). */
    public enum Granularity {
        ITEM,
        BATCH
    }

    protected static record InternalBatch<G, I>(long batchId, boolean isInNewThread, G groupKey, List<I> batch, List<Long> reverseMap) {}

    /**
     * Transaction-aware interface for the customizing iterator creation.
     * The begin and end methods are intended for transaction management.
     *
     * Each method is only invoked once and all methods are always invoked on the same thread.
     * The end method will be invoked upon closing the iterator or if iterator creation fails.
     */
    public interface IteratorCreator<T> {
        default void begin() {}
        AbortableIterator<T> createIterator();
        default void end() {}
    }

    static class IteratorWrapperViaThread<T, X extends Iterator<T>>
        extends AbstractAbortableIterator<T>
    {
        protected ExecutorServiceWrapperSync executorServiceSync;

        /** Number of items to transfer in batch from the worker thread to the calling thread */
        protected int batchSize;
        protected X delegate;

        protected volatile List<T> buffer;
        protected Iterator<T> currentBatch;

        /**
         * A function to detach items from the life-cycle of the iterator.
         * For example, TDB2 Bindings must be detached from resources that are free'd when the iterator is closed.
         */
        protected UnaryOperator<T> detachFn;

        public IteratorWrapperViaThread(X delegate, ExecutorService es, UnaryOperator<T> detachFn) {
            this(delegate, es, detachFn, 1);
        }

        public IteratorWrapperViaThread(X delegate, ExecutorService es, UnaryOperator<T> detachFn, int batchSize) {
            super();
            this.executorServiceSync = new ExecutorServiceWrapperSync(es);
            this.delegate = delegate;
            this.detachFn = detachFn;
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
                        T item = detachFn.apply(rawItem);
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

//    class IteratorWrapperScheduleNewTasks<T>
//        extends AbortableIteratorWrapper<T> {
//
//        public IteratorWrapperScheduleNewTasks(AbortableIterator<T> qIter) {
//            super(qIter);
//        }
//
//        @Override
//        protected boolean hasNextBinding() {
//            scheduleTasks();
//            return super.hasNextBinding();
//        }
//
//        @Override
//        protected T moveToNextBinding() {
//            scheduleTasks();
//            return super.moveToNextBinding();
//        }
//
//        void scheduleTasks() {
//
//        }
//    }

    /** A prefetch task with metadata about which batch id and corresponding input ids it serves. */
    static class PrefetchTaskForBatch<T, X extends AbortableIterator<T>>
        extends PrefetchTaskBase<T, X> {

        protected long batchId;

        /** The input ids in ascending order served by this task. Never empty. */
        protected List<Long> servedInputIds;

        protected List<Runnable> afterRunActions = new ArrayList<>();

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
        protected void afterRun() {
            afterRunActions.forEach(Runnable::run);
            super.afterRun();
        }

        public void addAfterRunAction(Runnable runnable) {
            this.afterRunActions.add(runnable);
        }

        @Override
        public String toString() {
            return "TaskId for batchId " + batchId + " with inputIds " + servedInputIds + " [" + state + (isStopRequested ? "aborted" : "") + "]: " + bufferedItems.size() + " items buffered.";
        }

        public static <T> PrefetchTaskForBatch<T, AbortableIterator<T>> empty(long closeInputId) {
            return new PrefetchTaskForBatch<>(AbortableIterators.empty(), 0, closeInputId, List.of(closeInputId), UnaryOperator.identity());
        }
    }

    /**
     * Return the id of the last input handled by the task.
     * Resources can be free'd after processing that input.
     */
    protected static long getCloseId(Granularity granularity, PrefetchTaskForBatch<?, ?> task) {
        long result = switch (granularity) {
            case ITEM -> task.getServedInputIds().get(task.getServedInputIds().size() - 1);
            case BATCH -> task.getBatchId();
        };
        return result;
    }

    /** Helper class for tracking running prefetch tasks. */
    static class TaskEntry<T, X extends AbortableIterator<T>> {
        protected PrefetchTaskForBatch<T, X> task;
        protected Closeable inThreadCloseAction;

        protected ExecutorService executorService;
        protected Closeable outThreadCloseAction;

        protected Future<?> future;

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

        public boolean isTerminated() {
            return task().getState().equals(PrefetchTaskBase.State.TERMINATED);
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
                    // Closing the iterator below submits the inThreadCloseAction to the worker thread.
                    // After the worker thread has terminated the outThreadCloseAction is run.
                    IteratorCloseable<T> threadIt = new IteratorWrapperViaThread<>(tmp, executorService, task.getCopyFn()) {
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
                throw new RuntimeException(throwable);
            }

            return peekIter;
        }

        /** Create a completed task. */
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
    protected int maxFetchAhead = 100; // Fetch ahead is for additional task slots once maxConcurrentTasks have completed.

    /**
     * Only the driver is allowed to read from batchIterator because of a possibly running transaction.
     * Buffer ahead is how many items to read (and detach) from batchIterator so that new tasks can be scheduled
     * asynchronously (i.e. without the driver thread having to interfere).
     */
    protected int maxBufferAhead = 100;

    /** If batch granularity is true then output is ordered according to the obtained batches.
     *  Otherwise, output is ordered according to the individual member ids of the batches (assumes ids are unique across batches). */
    protected Granularity granularity;
    protected AbortableIterator<GroupedBatch<G, Long, I>> batchIterator;

    protected long nextBatchId = 0; // Counter of items taken from batchIterator

    // Input iteration.
    protected long currentInputId = -1;
    protected TaskEntry<O, AbortableIterator<O>> activeTaskEntry = null; // Cached reference for inputToOutputIt.get(currentInputId).
    protected AbortableIteratorPeek<O> activeIter;

    /**
     * Tasks ordered by their input id - regardless of whether they are run concurrently or not.
     * The complexity here is that the multiple inputIds may map to the same task.
     * Conversely: A single task may serve multiple input ids.
     */
    protected Map<Long, TaskEntry<O, AbortableIterator<O>>> inputToOutputIt = new LinkedHashMap<>();

    // FIXME The InstanceLifeCycle can start a task but it does not provide the metadata of its task its.
    protected BlockingQueue<InternalBatch<G, I>> taskQueue;

    /* State for tracking concurrent prefetch ----------------------------- */

    protected ExecutorServicePool executorServicePool;

    private final int maxConcurrentTasks;
    private final long concurrentSlotReadAheadCount;


    // The id of next task that can be started.
    private final AtomicLong nextStartableTask = new AtomicLong();

    // Whenever a task is started, the count is incremented.
    // Tasks decrement the count themselves just before exiting.
    private final AtomicInteger freeTaskSlots = new AtomicInteger();

    /** The concurrently running tasks. Indexed once by the last input id (upon which to close). */
    // private final Map<Long, TaskEntry<O, AbortableIterator<O>>> openConcurrentTaskEntriesRunning = new ConcurrentHashMap<>();

    /**
     * Completed tasks are moved from the running map to this one by the driver thread.
     * This map is used to count the number of completed tasks and the remaining task slots.
     * This map is not used for resource management.
     */
    // private final Map<Long, TaskEntry<O, AbortableIterator<O>>> openConcurrentTaskEntriesCompleted = new ConcurrentHashMap<>();

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

        // Set up a dummy task with an empty iterator as the active one
        // (currentInputId set to -1) and ensure it gets properly closed.
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

        boolean didAdvanceActiveIter = false;
        // Peek the next binding on the active iterator and verify that it maps to the current
        // partition key.
        while (true) { // Note: Cancellation is handled by base class before calling moveToNext().
            if (activeIter.hasNext()) {
                O peek = activeIter.peek();
                // The iterator returns null if it was aborted.
                if (peek == null) {
                    break;
                }

                // On batch granularity always take the lowest input id served by a task.
                long peekOutputId = switch(granularity) {
                    case ITEM -> extractInputOrdinal(peek);
                    case BATCH -> activeTaskEntry.task().getBatchId();
                };

                boolean inputIdMatches = peekOutputId == currentInputId;
                if (inputIdMatches) {
                    result = activeIter.next();
                    break;
                }
            }

            // If we come here then we need to advance the lhs.
            didAdvanceActiveIter = true;

            // Cleanup of no longer needed resources
            long closeId = getCloseId(granularity, activeTaskEntry.task());
            boolean isClosePoint = currentInputId == closeId;
            if (isClosePoint) {
                activeIter.close();
                inputToOutputIt.remove(currentInputId);
                // FIXME Can the entry still reside in the running list? Perhaps running.remove(id) is not needed.
                openConcurrentTaskEntriesRunning.remove(currentInputId);
                openConcurrentTaskEntriesCompleted.remove(currentInputId);
            }

            // Move to the next inputId
            ++currentInputId; // TODO peekOutputId may not have matched currentInputId

            activeTaskEntry = inputToOutputIt.get(currentInputId);
            if (activeTaskEntry == null) {
                // No entry for the advanced inputId - check whether further batches need to be executed.
                prepareNextBatchExecs();

                activeTaskEntry = inputToOutputIt.get(currentInputId);

                // If there is still no further batch then we must have reached the end.
                if (activeTaskEntry == null) {
                    break;
                }
            }

            // Stop any concurrent prefetching and get the iterator.
            activeIter = activeTaskEntry.stopAndGet();
        }

        if (result == null) {
            result = endOfData();
        } else {
            if (!didAdvanceActiveIter) {
                // Check whether to schedule any further concurrent tasks.
                // Check is redundant if activeIter was advanced.
                prepareNextBatchExecs();
            }
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

    /** Method called from worker threads to start execution. */
    protected void startNextTasks() {
        // If there are more than 0 free slots then decrement the count; otherwise stay at 0.
        int freeSlots = freeTaskSlots.getAndUpdate(i -> Math.max(0, i - 1));
        while (freeSlots > 0) {
            // Need to ensure the queue is not empty.
            // startableTasks.
            Object task = startableTasks.poll();
            if (task == null) {
                // Nothing to execute - free the slot again
                freeTaskSlots.incrementAndGet();
            }

            // launch the task
        }
    }

    public void prepareNextBatchExecs() {
        if (!inputToOutputIt.containsKey(currentInputId)) {
            // We need the task's iterator right away - do not start concurrent retrieval
            if (batchIterator.hasNext()) {
                InternalBatch<G, I> batch = nextBatch(false);
                InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(batch);
                PrefetchTaskForBatch<O, AbortableIterator<O>> task = taskLifeCycle.newInstance();
                 TaskEntry<O, AbortableIterator<O>> taskEntry = TaskEntry.completed(task, () -> taskLifeCycle.closeInstance(task));
                registerTaskEntry(taskEntry);
            }
        }

        // TODO Allow preparation of tasks without starting them


        // Give the implementation a chance to reject concurrent execution via an exception.
        // For example, if a READ_PROMOTE transaction switched to WRITE than concurrent READ might cause a deadlock.
        // FIXME Only check if there are free concurrent slots
        checkCanExecInNewThread();

        // Fill any remaining slots in the task queue for concurrent processing
        // Concurrent tasks have their own execution contexts because execCxt is not thread safe.

        drainCompletedTasks(openConcurrentTaskEntriesCompleted, openConcurrentTaskEntriesRunning);
        int runningTasks = openConcurrentTaskEntriesRunning.size();
        int freeTaskSlots = Math.max(maxConcurrentTasks - runningTasks, 0);
        int completedTasks = openConcurrentTaskEntriesCompleted.size();

        int usedFetchAheadSlots = Math.max(completedTasks - maxConcurrentTasks, 0);
        int remainingFetchAheadSlots = Math.max(maxFetchAhead - usedFetchAheadSlots, 0);

        int freeSlots = Math.min(freeTaskSlots, remainingFetchAheadSlots);

        int i;
        for (i = 0; i < freeSlots && batchIterator.hasNext() && !isCancelled(); ++i) {

            InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(true);
            ExecutorService executorService = executorServicePool.acquireExecutor();

            // Create the task through the execution thread such that thread locals (e.g. for transactions)
            // are initialized on the correct thread
            PrefetchTaskForBatch<O, AbortableIterator<O>> task = ExecutorServiceWrapperSync.submit(executorService, taskLifeCycle::newInstance);

//            // In the task thread, if the run method completes then check whether any further tasks can be scheduled.
//            task.addAfterRunAction(() -> {
//                // If the iterator was consumed then free a slot.
//                if (!task.getIterator().hasNext()) {
//                    freeTaskSlotCount.inc();
//                }
//            });

            long closeId = getCloseId(granularity, task);

            // Submit the task which immediately starts concurrent retrieval.
            // Retrieval happens on the same thread on which the task was created.
            Future<?> future = executorService.submit(task);
            TaskEntry<O, AbortableIterator<O>> taskEntry = new TaskEntry<>(
                task,
                () -> taskLifeCycle.closeInstance(task),
                executorService,
                () -> {
                    // This action returns the acquired executor service back to the pool
                    executorService.shutdown();
                },
                future);

            registerTaskEntry(taskEntry);
            openConcurrentTaskEntriesRunning.put(closeId, taskEntry);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("completedTasks: {}, runningTasks: {}, newlySubmittedTasks: {}, freeSlots: {} (freeTaskSlots: {}, freeReadAheadSlots: {})",
                completedTasks, runningTasks, i, freeSlots, freeTaskSlots, remainingFetchAheadSlots);
        }
    }

    protected void fillTaskQueue() {
        while (batchIterator.hasNext()) {
            InternalBatch<G, I> batch = nextBatch(true);
            taskQueue.add(batch);
        }
    }

    /**
     * Remove completed tasks from 'running' and add them to 'completed'.
     *
     * @return The number of completed (and thus transferred) tasks.
     */
    protected static <O> int drainCompletedTasks(Map<Long, TaskEntry<O, AbortableIterator<O>>> completed, Map<Long, TaskEntry<O, AbortableIterator<O>>> running) {
        int newCompletedTasks = 0;
        Iterator<Entry<Long, TaskEntry<O, AbortableIterator<O>>>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, TaskEntry<O, AbortableIterator<O>>> e = it.next();
            boolean isTerminated = e.getValue().isTerminated();
            if (isTerminated) {
                ++newCompletedTasks;
                it.remove();
                completed.put(e.getKey(), e.getValue());
            }
        }
        return newCompletedTasks;
    }

    protected abstract boolean isCancelled();

    protected abstract IteratorCreator<O> processBatch(boolean isInNewThread, G groupKey, List<I> batch, List<Long> reverseMap);

    protected abstract long extractInputOrdinal(O input);
    protected abstract void checkCanExecInNewThread();

    protected I detachInput(I item, boolean isInNewThread) { return item; }
    protected O detachOutput(O item, boolean isInNewThread) { return item; }

    /** Consume the next batch from */
    protected InternalBatch<G, I> nextBatch(boolean isInNewThread) {
        GroupedBatch<G, Long, I> batchRequest = batchIterator.next();
        long batchId = nextBatchId++;
        Batch<Long, I> batch = batchRequest.getBatch();
        NavigableMap<Long, I> batchItems = batch.getItems();

        G groupKey = batchRequest.getGroupKey();

        // Detach inputs.
        Collection<I> rawInputs = batchItems.values();
        List<I> detachedInputs = new ArrayList<>(rawInputs.size());
        rawInputs.forEach(rawInput -> detachedInputs.add(detachInput(rawInput, isInNewThread)));
        List<I> inputs = Collections.unmodifiableList(detachedInputs);

        List<Long> reverseMap = List.copyOf(batchItems.keySet());

        InternalBatch<G, I> result = new InternalBatch<>(batchId, isInNewThread, groupKey, inputs, reverseMap);
        return result;
    }

    /** Prepare the lazy execution of the next batch and register all iterators with {@link #inputToOutputIt} */
    // seqId = sequential number injected into the request
    // inputId = id (index) of the input binding
    // rangeId = id of the range w.r.t. to the input binding
    // partitionKey = (inputId, rangeId)
    protected InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> prepareNextBatchExec(InternalBatch<G, I> batch) {
//        GroupedBatch<G, Long, I> batchRequest = batchIterator.next();
//        long batchId = nextBatchId++;
//        Batch<Long, I> batch = batchRequest.getBatch();
//        NavigableMap<Long, I> batchItems = batch.getItems();
//
//        G groupKey = batchRequest.getGroupKey();
//
//        // Detach inputs.
//        Collection<I> rawInputs = batchItems.values();
//        List<I> detachedInputs = new ArrayList<>(rawInputs.size());
//        rawInputs.forEach(rawInput -> detachedInputs.add(detachInput(rawInput, isInNewThread)));
//        List<I> inputs = Collections.unmodifiableList(detachedInputs);
//
//        List<Long> reverseMap = List.copyOf(batchItems.keySet());
//
//        InternalBatch<G, I> internalBatch = new InternalBatch<>(groupKey, inputs, reverseMap);
        long batchId = batch.batchId();
        boolean isInNewThread = batch.isInNewThread();
        G groupKey = batch.groupKey();
        List<I> inputs = batch.batch();
        List<Long> reverseMap = batch.reverseMap();

        IteratorCreator<O> creator = processBatch(isInNewThread, groupKey, inputs, reverseMap);

        UnaryOperator<O> detachItemFn = x -> detachOutput(x, isInNewThread);

        InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> result = InstanceLifeCycles.of(() -> {
            creator.begin();
            AbortableIterator<O> tmp = creator.createIterator();
            PrefetchTaskForBatch<O, AbortableIterator<O>> task = new PrefetchTaskForBatch<>(tmp, concurrentSlotReadAheadCount, batchId, reverseMap, detachItemFn);
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
        AbortableIteratorBase.performRequestCancel(activeIter);
    }

    /** Check whether the values in the given collection are consecutive. */
    /*
    private static <T extends Comparable<T>> boolean isConsecutive(Iterator<T> it, DiscreteDomain<T> domain) {
        boolean result = true;
        if (it.hasNext()) {
            T start = it.next();
            T expected = domain.next(start);
            while (it.hasNext()) {
                T actual = it.next();
                if (!Objects.equals(expected, actual)) {
                    // XXX Better return a violation report than just a boolean.
                    result = false;
                    break;
                }
                expected = domain.next(expected);
            }
        }
        return result;
    }
    */
}
