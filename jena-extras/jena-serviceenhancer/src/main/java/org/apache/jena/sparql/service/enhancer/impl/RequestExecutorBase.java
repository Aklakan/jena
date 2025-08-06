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
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.service.enhancer.concurrent.ExecutorServicePool;
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

    /**
     * A prefetch task with metadata about which batch id and corresponding input ids it serves.
     *
     * @param <T> Type of the items of the (abortable) iterator.
     */
//    static class PrefetchTaskForBatch<T>
//        extends PrefetchTaskBase<T, AbortableIterator<T>> {
//
//        protected long batchId;
//
//        /** The input ids in ascending order served by this task. Never empty. */
//        protected List<Long> servedInputIds;
//
//        protected List<Runnable> afterRunActions = new ArrayList<>();
//
//        public PrefetchTaskForBatch(AbortableIterator<T> iterator, long maxBufferedItemsCount, long batchId, List<Long> servedInputIds, UnaryOperator<T> copyFn) {
//            super(iterator, maxBufferedItemsCount, copyFn);
//            this.batchId = batchId;
//            this.servedInputIds = Objects.requireNonNull(servedInputIds);
//
//            if (servedInputIds.isEmpty()) {
//                throw new IllegalArgumentException("Input ids must be neither null nor empty");
//            }
//        }
//
//        long getBatchId() {
//            return batchId;
//        }
//
//        public List<Long> getServedInputIds() {
//            return servedInputIds;
//        }
//
//        @Override
//        protected void afterRun() {
//            afterRunActions.forEach(Runnable::run);
//            super.afterRun();
//        }
//
//        public void addAfterRunAction(Runnable runnable) {
//            this.afterRunActions.add(runnable);
//        }
//
//        @Override
//        public String toString() {
//            return "TaskId for batchId " + batchId + " with inputIds " + servedInputIds + " [" + state + (isStopRequested ? "aborted" : "") + "]: " + bufferedItems.size() + " items buffered.";
//        }
//
//        public static <T> PrefetchTaskForBatch<T> empty(long closeInputId) {
//            return new PrefetchTaskForBatch<>(AbortableIterators.empty(), 0, closeInputId, List.of(closeInputId), UnaryOperator.identity());
//        }
//    }

    /**
     * Return the id of the last input handled by the task.
     * Resources can be free'd after processing that input.
     */
//    protected static long getCloseId(Granularity granularity, PrefetchTaskBase<?, ?> task) {
//        long result = switch (granularity) {
//            case ITEM -> task.getServedInputIds().get(task.getServedInputIds().size() - 1);
//            case BATCH -> task.getBatchId();
//        };
//        return result;
//    }

    protected long getCloseId(Granularity granularity, TaskEntry<?> taskEntry) {
        long result = switch (granularity) {
            case ITEM -> taskEntry.getServedInputIds().get(taskEntry.getServedInputIds().size() - 1);
            case BATCH -> taskEntry.getBatchId();
        };
        return result;
    }


//    class TaskEntry {
//        protected InternalBatch<G, I> batch;
//        public long getBatchId() {
//            return batch.batchId();
//        }
//
//        public List<Long> getServedInputIds() {
//            return batch.reverseMap();
//            // return servedInputIds;
//        }
//    }

    interface TaskEntry<O>
        extends AutoCloseable
    {
        long getBatchId();
        List<Long> getServedInputIds();
        AbortableIteratorPeek<O> stopAndGet();
        void close();
    }

    class TaskEntryEmpty
        implements TaskEntry<O>
    {
        private long closeId;
        private List<Long> servedInputIds;

        public TaskEntryEmpty(long closeId) {
            super();
            this.closeId = closeId;
            this.servedInputIds = List.of(closeId);
        }

        @Override
        public long getBatchId() {
            return closeId;
        }

        @Override
        public List<Long> getServedInputIds() {
            return servedInputIds;
        }

        @Override
        public AbortableIteratorPeek<O> stopAndGet() {
            return new AbortableIteratorPeek<>(AbortableIterators.empty());
        }

        @Override
        public void close() {
        }
    }

    abstract class TaskEntryBatchBase
        implements TaskEntry<O>
    {
        protected InternalBatch<G, I> batch;

        public TaskEntryBatchBase(InternalBatch<G, I> batch) {
            super();
            this.batch = batch;
        }

        @Override
        public long getBatchId() {
            return batch.batchId();
        }

        @Override
        public List<Long> getServedInputIds() {
            return batch.reverseMap();
        }
    }

    class TaskEntryDirect
        extends TaskEntryBatchBase
    {
        protected IteratorCreator<O> iteratorCreator;
        protected AbortableIteratorPeek<O> createdIterator;

        public TaskEntryDirect(InternalBatch<G, I> batch) {
            super(batch);
        }

        @Override
        public AbortableIteratorPeek<O> stopAndGet() {
            if (iteratorCreator == null) {
                iteratorCreator = processBatch(batch);
                iteratorCreator.begin();
                createdIterator = new AbortableIteratorPeek<>(iteratorCreator.createIterator());
            }
            return createdIterator;
        }

        @Override
        public void close() {
            if (iteratorCreator != null) {
                iteratorCreator.end();
            }
        }
    }


    // There are three ways to execute sub-iterators:
    // EmptyTask - a dummy task that has a closeId but does not produce items
    // DirectTask - return an iterator directly on the driver thread
    // AsyncTask - items are prefetched until the driver calls "stop". for scattered data the driver could actually restart the fetch-ahead again... but let's not do that now. probably it would be relatively easy to add later.

    /** Helper class for tracking running prefetch tasks. */
    // TODO Free the task once the thread exits.
    //   What happens if the task itself gives back the executor?! -> The executor will be simply handed back to the pool, but the task will exit only shortly after.
    //   This means that the executor may be briefly occupied which should be harmless.
    class TaskEntryAsync
        extends TaskEntryBatchBase
    {
        public TaskEntryAsync(InternalBatch<G, I> batch) {
            super(batch);
        }

        protected volatile PrefetchTaskBase<O, AbortableIterator<O>> task;
        protected volatile ExecutorService executorService;
        protected volatile Future<?> future;
        protected volatile AbortableIteratorPeek<O> peekIter;

        // protected Closeable inThreadCloseAction;
        // protected Closeable outThreadCloseAction;

//        public TaskEntry(InternalBatch<G, I> batch, Function<InternalBatch<G, I>, IteratorCreator<O>> batchToIteratorCreator) {// , Closeable inThreadCloseAction, ExecutorService executorService, Closeable outThreadCloseAction, Future<?> future) {
//            super();
//            this.batch = batch;
//            this.batchToIteratorCreator = batchToIteratorCreator;
//
////            this.task = task;
////            this.inThreadCloseAction = inThreadCloseAction;
////            this.executorService = executorService;
////            this.outThreadCloseAction = outThreadCloseAction;
////            this.future = future;
//        }

        public PrefetchTaskBase<O, AbortableIterator<O>> task() {
            return task;
        }

        public Future<?> future() {
            return future;
        }

        public boolean isTerminated() {
            return task().getState().equals(PrefetchTaskBase.State.TERMINATED);
        }

        public void startInNewThread() {
            boolean isInNewThread = true;

            IteratorCreator<O> creator = processBatch(batch);
            UnaryOperator<O> detachItemFn = x -> detachOutput(x, isInNewThread);

            Callable<PrefetchTaskBase<O, AbortableIterator<O>>> createOutputIt = () -> {
                creator.begin();
                AbortableIterator<O> tmp = creator.createIterator();

                PrefetchTaskBase<O, AbortableIterator<O>> task = new PrefetchTaskBase<>(tmp, concurrentSlotReadAheadCount, detachItemFn) {
                    @Override
                    protected void afterRun() {
                        // If all data was consumed then free resources.
                        if (!iterator.hasNext()) {
                            freeResources();
                            creator.end();
                        }
                        fillTaskQueue();
                    }
                };

                // PrefetchTaskForBatch<O> task = new PrefetchTaskForBatch<>(tmp, concurrentSlotReadAheadCount, batchId, reverseMap, detachItemFn);
                return task;
            };

//            Consumer<PrefetchTaskBase<O, AbortableIterator<O>>> closeOutputIt = task -> {
//                try {
//                    task.getIterator().close();
//                } finally {
//                    creator.end();
//                }
//            };

            ExecutorService executorService = executorServicePool.acquireExecutor();

            // Create the task through the execution thread such that thread locals (e.g. for transactions)
            // are initialized on the correct thread
            PrefetchTaskBase<O, AbortableIterator<O>> task = ExecutorServiceWrapperSync.submit(executorService, createOutputIt::call);

            future = executorService.submit(task);
            // inThreadCloseAction = () -> closeOutputIt.accept(task);
//            outThreadCloseAction = () -> executorService.shutdown();
//
////            // In the task thread, if the run method completes then check whether any further tasks can be scheduled.
////            task.addAfterRunAction(() -> {
////                // If the iterator was consumed then free a slot.
////                if (!task.getIterator().hasNext()) {
////                    freeTaskSlotCount.inc();
////                }
////            });
//
//            long closeId = getCloseId(granularity, this);
//
//            // Submit the task which immediately starts concurrent retrieval.
//            // Retrieval happens on the same thread on which the task was created.
//
        }


        /**
         * Stop the task and return an iterator over the buffered items and the remaining ones.
         * Closing the returned iterator also closes the iterator over the remaining items.
         *
         * In case of an exception, this task entry closes itself.
         */
        public AbortableIteratorPeek<O> stopAndGet() {
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

                AbortableIterator<O> tmp = task.getIterator();

                // If there is an executorService then make sure the iterator is accessed through it.
                // Closing the iterator below submits the inThreadCloseAction to the worker thread.
                // After the worker thread has terminated the outThreadCloseAction is run.
                AbortableIterator<O> threadIt = new IteratorWrapperViaThread<>(tmp, executorService, task.getCopyFn()); // {
                    /** The close action runs inside of the executor service. */
//                    @Override
//                    protected void inThreadCloseAction() {
//                        try {
//                            super.inThreadCloseAction();
//                        } finally {
//                            if (inThreadCloseAction != null) {
//                                inThreadCloseAction.close();
//                            }
//                        }
//                    }
//
//                    @Override
//                    protected void outThreadCloseAction() {
//                        if (outThreadCloseAction != null) {
//                            outThreadCloseAction.close();
//                        }
//                    }
//                };
                // tmp = AbortableIterators.wrap(threadIt);

                // If there are buffered items then prepend them to 'tmp'
                List<O> bufferedItems = task.getBufferedItems();
                if (!bufferedItems.isEmpty()) {
                    @SuppressWarnings("resource") // 'concat' will eventually be closed by the calling thread
                    AbortableIteratorConcat<O> concat = new AbortableIteratorConcat<>();
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

        protected void freeResources() {
            // TODO Skip if already freed.
            executorService.submit(() -> creator.end());

            executorService.shutdown();
            freeTaskSlots.incrementAndGet();
        }

        @Override
        public void close() {
            freeResources();
        }

        /** Create a completed task. */
//        public static <T, X extends AbortableIterator<T>> TaskEntry<T, X> completed(PrefetchTaskForBatch<T, X> task, Closeable closeAction) {
//            return new TaskEntry<>(
//                task,
//                closeAction,
//                null,
//                null,
//                CompletableFuture.completedFuture(null)
//            );
//        }

////        public static <T> TaskEntry<T, AbortableIterator<T>> empty(long closeInputId) {
////            return completed(PrefetchTaskForBatch.empty(closeInputId), null);
////        }
//        public static <T> TaskEntry empty(long closeInputId) {
//            InternalBatch<G, I>
//
//            return new TaskEntry(null);
//
//            // Function<Internal IteratorCreator<T> emptyItCreator = AbortableIterators::empty;
//            // PrefetchTaskBase<Object, AbortableIterator<Object>> prefetchTask = PrefetchTaskBase.of(AbortableIterators.empty(), 1 /* maxBufferedItemCount */, UnaryOperator.identity());
//            // return new PrefetchTaskForBatch<>(AbortableIterators.empty(), 0, closeInputId, List.of(closeInputId), UnaryOperator.identity());
//            return completed(PrefetchTaskForBatch.empty(closeInputId), null);
//        }
    }

//    protected TaskEntry emptyTaskEntry(long closeInputId) {
//        List<I> dummyInput = new ArrayList<>(1);
//        dummyInput.add(null);
//        // (long batchId, boolean isInNewThread, G groupKey, List<I> batch, List<Long> reverseMap) {}
//        InternalBatch<G, I> batch = new InternalBatch<>(closeInputId, false, null, dummyInput, List.of(closeInputId));
//        IteratorCreator<O> itCreator = AbortableIterators::empty;
//        return new TaskEntry(batch, b -> itCreator);
//    }

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
    protected TaskEntry<O> activeTaskEntry = null; // Cached reference for inputToOutputIt.get(currentInputId).
    protected AbortableIteratorPeek<O> activeIter;

    /**
     * Tasks ordered by their input id - regardless of whether they are run concurrently or not.
     * The complexity here is that the multiple inputIds may map to the same task.
     * Conversely: A single task may serve multiple input ids.
     */
    protected Map<Long, TaskEntry<O>> inputToOutputIt = new LinkedHashMap<>();

    /** The task queue is used to submit the tasks in "inputToOutputIt" to executors. */
    protected BlockingQueue<TaskEntryAsync> taskQueue;

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
        activeTaskEntry = new TaskEntryEmpty(currentInputId); // emptyTaskEntry(currentInputId); //TaskEntry.empty(currentInputId);
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
                    case BATCH -> activeTaskEntry.getBatchId();
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
            long closeId = getCloseId(granularity, activeTaskEntry);
            boolean isClosePoint = currentInputId == closeId;
            if (isClosePoint) {
                activeIter.close();
                activeTaskEntry.close();
                inputToOutputIt.remove(currentInputId);
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

    protected void registerTaskEntry(TaskEntry<O> taskEntry) {
        switch (granularity) {
        case ITEM:
            List<Long> servedInputIds = taskEntry.getServedInputIds();
            for (Long e : servedInputIds) {
                inputToOutputIt.put(e, taskEntry);
            }
            break;
        case BATCH:
            long batchId = taskEntry.getBatchId();
            inputToOutputIt.put(batchId, taskEntry);
            break;
        default:
            throw new IllegalStateException("Should never come here.");
        }
    }

    public void prepareNextBatchExecs() {
        // FIXME There may be a task in the task queue.
        // Ideally the driver would already create the entry in inputToOutputMap.
        // and the executors would just call a start() method on the tasks.

        if (!inputToOutputIt.containsKey(currentInputId)) {
            // We need the task's iterator right away - do not start concurrent retrieval
            if (batchIterator.hasNext()) {
                // InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(false);
                InternalBatch<G, I> batch = nextBatch(false);
                TaskEntry<O> taskEntry = new TaskEntryDirect(batch);
                registerTaskEntry(taskEntry);
                // TaskEntry<O> taskEntry = prepareNextBatchExec(false);
                // taskEntry.startCompleted();
            }
        }

        // TODO Allow preparation of tasks without starting them
        fillTaskQueue();
        processTaskQueue();

//
//        // Give the implementation a chance to reject concurrent execution via an exception.
//        // For example, if a READ_PROMOTE transaction switched to WRITE than concurrent READ might cause a deadlock.
//        // FIXME Only check if there are free concurrent slots
//        checkCanExecInNewThread();
//
//        // Fill any remaining slots in the task queue for concurrent processing
//        // Concurrent tasks have their own execution contexts because execCxt is not thread safe.
//
//        drainCompletedTasks(openConcurrentTaskEntriesCompleted, openConcurrentTaskEntriesRunning);
//        int runningTasks = openConcurrentTaskEntriesRunning.size();
//        int freeTaskSlots = Math.max(maxConcurrentTasks - runningTasks, 0);
//        int completedTasks = openConcurrentTaskEntriesCompleted.size();
//
//        int usedFetchAheadSlots = Math.max(completedTasks - maxConcurrentTasks, 0);
//        int remainingFetchAheadSlots = Math.max(maxFetchAhead - usedFetchAheadSlots, 0);
//
//        int freeSlots = Math.min(freeTaskSlots, remainingFetchAheadSlots);
//
//        int i;
//        for (i = 0; i < freeSlots && batchIterator.hasNext() && !isCancelled(); ++i) {
//
//            InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(true);
//            ExecutorService executorService = executorServicePool.acquireExecutor();
//
//            // Create the task through the execution thread such that thread locals (e.g. for transactions)
//            // are initialized on the correct thread
//            PrefetchTaskForBatch<O, AbortableIterator<O>> task = ExecutorServiceWrapperSync.submit(executorService, taskLifeCycle::newInstance);
//
////            // In the task thread, if the run method completes then check whether any further tasks can be scheduled.
////            task.addAfterRunAction(() -> {
////                // If the iterator was consumed then free a slot.
////                if (!task.getIterator().hasNext()) {
////                    freeTaskSlotCount.inc();
////                }
////            });
//
//            long closeId = getCloseId(granularity, task);
//
//            // Submit the task which immediately starts concurrent retrieval.
//            // Retrieval happens on the same thread on which the task was created.
//            Future<?> future = executorService.submit(task);
//            TaskEntry<O, AbortableIterator<O>> taskEntry = new TaskEntry<>(
//                task,
//                () -> taskLifeCycle.closeInstance(task),
//                executorService,
//                () -> {
//                    // This action returns the acquired executor service back to the pool
//                    executorService.shutdown();
//                },
//                future);
//
//            // InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(batch);
//            // PrefetchTaskForBatch<O, AbortableIterator<O>> task = taskLifeCycle.newInstance();
//            // TaskEntry<O, AbortableIterator<O>> taskEntry = TaskEntry.completed(task, () -> taskLifeCycle.closeInstance(task));
//            registerTaskEntry(taskEntry);
//
//            // openConcurrentTaskEntriesRunning.put(closeId, taskEntry);
//        }
//        if (logger.isDebugEnabled()) {
//            logger.debug("completedTasks: {}, runningTasks: {}, newlySubmittedTasks: {}, freeSlots: {} (freeTaskSlots: {}, freeReadAheadSlots: {})",
//                completedTasks, runningTasks, i, freeSlots, freeTaskSlots, remainingFetchAheadSlots);
//        }
    }

    /** This method is only called from the driver. */
    protected void fillTaskQueue() {
        while (batchIterator.hasNext()) {
            // FIXME Make sure there is capacity left in the task queue

            // TODO Use statistics to decide how far to read ahead.
            // Track the maximum number of tasks that complete for item in the driver.
            // If all tasks were consumed then double the number of threads - up to some cap.

            InternalBatch<G, I> batch = nextBatch(true);
            TaskEntryAsync taskEntry = new TaskEntryAsync(batch);
//            TaskEntry taskEntry = new TaskEntry(batch);
            // TaskEntry<O taskEntry = prepareNextBatchExec(true);
            taskQueue.offer(taskEntry);
        }
    }

    /** Method called from worker threads to start execution. */
    protected void processTaskQueue() {
        // If there are more than 0 free slots then decrement the count; otherwise stay at 0.
        int freeSlots = freeTaskSlots.getAndUpdate(i -> Math.max(0, i - 1));
        while (freeSlots > 0) {
            // Need to ensure the queue is not empty.
            TaskEntryAsync taskEntry = taskQueue.poll();
            if (taskEntry == null) {
                // Nothing to execute - free the slot again
                freeTaskSlots.incrementAndGet();
            } else {
                checkCanExecInNewThread();

                // Launch the task. The task is set up to call "freeTaskSlots.incrementAndGet()" upon completion.
                taskEntry.startInNewThread();
            }
        }
    }

    protected abstract boolean isCancelled();

    protected IteratorCreator<O> processBatch(InternalBatch<G, I> batch) {
        boolean isInNewThread = batch.isInNewThread();
        // long batchId = batch.batchId();
        G groupKey = batch.groupKey();
        List<I> inputs = batch.batch();
        List<Long> reverseMap = batch.reverseMap();
        IteratorCreator<O> result = processBatch(isInNewThread, groupKey, inputs, reverseMap);
        return result;
    }

    protected abstract IteratorCreator<O> processBatch(boolean isInNewThread, G groupKey, List<I> batch, List<Long> reverseMap);

    protected abstract long extractInputOrdinal(O input);
    protected abstract void checkCanExecInNewThread();

    protected I detachInput(I item, boolean isInNewThread) { return item; }
    protected O detachOutput(O item, boolean isInNewThread) { return item; }

    /** Consume the next batch from the batchIterator. */
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
    // protected InstanceLifeCycle<PrefetchTaskForBatch<O, AbortableIterator<O>>> prepareNextBatchExec(InternalBatch<G, I> batch) {
//    protected TaskEntry<O> prepareNextBatchExec(boolean isInNewThread) {
//        InternalBatch<G, I> batch = nextBatch(isInNewThread);
//        TaskEntry taskEntry = new TaskEntry(batch, this::processBatch);
//        return taskEntry;
//    }

    protected void freeResources() {
        // Use closer to free as much as possible in case of failure.
        try (Closer closer = Closer.create()) {
            closer.register(activeIter::close);

            for (TaskEntry taskEntry : inputToOutputIt.values()) {
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
}
