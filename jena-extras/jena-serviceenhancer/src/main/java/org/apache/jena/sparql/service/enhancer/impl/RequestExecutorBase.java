package org.apache.jena.sparql.service.enhancer.impl;

import java.util.ArrayList;
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

/** A flat map iterator that can read ahead in the input iterator and run flat map operations concurrently. */
public abstract class RequestExecutorBase<G, I, O>
    extends AbstractAbortableIterator<O>
{
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

                if (currentBatch.hasNext()) {
                    result = currentBatch.next();
                } else {
                    result = null;
                }
            }
            return result;
        }

        /**
         * Close the iterator.
         * Note that the worker is blocked while retrieving so in that case any close signal won't get through.
         */
        @Override
        public final void closeIterator() {
            executorServiceSync.submit(this::inThreadCloseAction);
            outThreadCloseAction();
        }

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

    static class PrefetchTaskForBinding<T, X extends AbortableIterator<T>>
        extends PrefetchTask<T, X> {

        /** The input ids in ascending order served by this task. Never empty. */
        protected List<Long> servedInputIds;

        /** The list item in {@link #servedInputIds}. */
        protected long closeInputId;

        public PrefetchTaskForBinding(X iterator, long maxBufferedItemsCount, List<Long> servedInputIds, UnaryOperator<T> copyFn) {
            super(iterator, maxBufferedItemsCount, copyFn);

            this.servedInputIds = Objects.requireNonNull(servedInputIds);
            this.closeInputId = servedInputIds.get(servedInputIds.size() - 1);

            if (servedInputIds.isEmpty()) {
                throw new IllegalArgumentException("Input ids must be neither null nor empty");
            }
        }

        public List<Long> getServedInputIds() {
            return servedInputIds;
        }

        public long getCloseInputId() {
            return closeInputId;
        }

        @Override
        public String toString() {
            return "TaskId for inputIds " + servedInputIds + " [" + state + (isStopRequested ? "aborted" : "") + "]: " + bufferedItems.size() + " items buffered.";
        }

        public static <T> PrefetchTaskForBinding<T, AbortableIterator<T>> empty(long closeInputId) {
            return new PrefetchTaskForBinding<>(AbortableIterators.empty(), 0, List.of(closeInputId), UnaryOperator.identity());
        }
    }

    /** Helper record for tracking running prefetch tasks. */
    static class TaskEntry<T, X extends AbortableIterator<T>> {
        // protected LifeCycle<PrefetchTaskForBinding> taskLifeCycle;
        protected PrefetchTaskForBinding<T, X> task;
        protected Closeable inThreadCloseAction;

        protected ExecutorService executorService;
        protected Closeable outThreadCloseAction;

        protected Future<?> future;
        // protected ExecutionContext execCxt;

        protected volatile AbortableIteratorPeek<T> peekIter;

        public TaskEntry(PrefetchTaskForBinding<T, X> task, Closeable inThreadCloseAction, ExecutorService executorService, Closeable outThreadCloseAction, Future<?> future) {
            super();
            this.task = task;
            this.inThreadCloseAction = inThreadCloseAction;
            this.executorService = executorService;
            this.outThreadCloseAction = outThreadCloseAction;
            this.future = future;
            // this.execCxt = execCxt;
        }

//      public ExecutionContext getExecCxt() {
//          return execCxt;
//      }

        public PrefetchTaskForBinding<T, X> task() {
            return task;
        }

        public Future<?> future() {
            return future;
        }

        /**
         * Stop the task and return an iterator over the buffered items and the remaining ones.
         * Closing the returned iterator also closes the iterator over the remaining items
         */
        public AbortableIteratorPeek<T> stopAndGet() {
            if (peekIter == null) {
                // Send the abort signal
                task.stop();
                // Wait for the task to complete
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
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
            return peekIter;
        }

        public static <T, X extends AbortableIterator<T>> TaskEntry<T, X> completed(PrefetchTaskForBinding<T, X> task, Closeable closeAction) {
            return new TaskEntry<>(
                task,
                closeAction,
                null,
                null,
                CompletableFuture.completedFuture(null)
            );
        }
        public static <T> TaskEntry<T, AbortableIterator<T>> empty(long closeInputId) {
            return completed(PrefetchTaskForBinding.empty(closeInputId), null);
        }
    }

    /**  Ensure that at least there are active requests to serve the next n input bindings */
    protected int fetchAhead = 5;
    protected int maxRequestSize = 2000;

    protected AbortableIterator<GroupedBatch<G, Long, I>> batchIterator;

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
            AbortableIterator<GroupedBatch<G, Long, I>> batchIterator,
            int maxConcurrentTasks,
            long concurrentSlotReadAheadCount) {
        super(cancelSignal);
        // super(execCxt);
        // this.useLoopJoin = useLoopJoin;
        this.batchIterator = Objects.requireNonNull(batchIterator);

//      Set<Var> visibleServiceSubOpVars = serviceInfo.getVisibleSubOpVarsScoped();
//      this.globalIdxVar = VarUtilsExtra.freshVar("__idx__", visibleServiceSubOpVars);

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
        // I parentBinding = null;
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

                long peekOutputId = extractLocalInputId(peek);
                // long peekOutputId = BindingUtils.getNumber(peek, globalIdxVar).longValue();

                boolean inputIdMatches = peekOutputId == currentInputId;
                if (inputIdMatches) {
                    // parentBinding = inputToBinding.get(currentInputId);
                    result = activeIter.next();
                    break;
                }
            }

            // Cleanup of no longer needed resources
            boolean isClosePoint = currentInputId == activeTaskEntry.task().getCloseInputId();
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

        return result;
    }

    protected void registerTaskEntry(TaskEntry<O, AbortableIterator<O>> taskEntry) {
        List<Long> servedInputIds = taskEntry.task().getServedInputIds();
        for (Long e : servedInputIds) {
            inputToOutputIt.put(e, taskEntry);
        }
    }

    public void prepareNextBatchExecs() {
        if (!inputToOutputIt.containsKey(currentInputId)) {
            // We need the task's iterator right away - do not start concurrent retrieval
            if (batchIterator.hasNext()) {
                InstanceLifeCycle<PrefetchTaskForBinding<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(false);
                PrefetchTaskForBinding<O, AbortableIterator<O>> task = taskLifeCycle.newInstance();
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
            InstanceLifeCycle<PrefetchTaskForBinding<O, AbortableIterator<O>>> taskLifeCycle = prepareNextBatchExec(true);
            ExecutorService executorService = executorServicePool.acquireExecutor(); //.acquireExecutor(); // Executors.newSingleThreadExecutor();

            // Create the task through the execution thread such that thread locals (e.g. for transactions)
            // are initialized on the correct thread
            PrefetchTaskForBinding<O, AbortableIterator<O>> task = ExecutorServiceWrapperSync.submit(executorService, taskLifeCycle::newInstance);

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
            openConcurrentTaskEntries.put(taskEntry.task().getCloseInputId(), taskEntry);
        }
    }

    protected abstract boolean isCancelled();

    protected abstract IteratorCreator<O> processBatch(boolean isInNewThread, G groupKey, List<I> batch, List<Long> reverseMap);

    protected abstract long extractLocalInputId(O input);
    protected abstract void checkCanExecInNewThread();

    protected O copy(O item) { return item; }

    /** Prepare the lazy execution of the next batch and register all iterators with {@link #inputToOutputIt} */
    // seqId = sequential number injected into the request
    // inputId = id (index) of the input binding
    // rangeId = id of the range w.r.t. to the input binding
    // partitionKey = (inputId, rangeId)
    protected InstanceLifeCycle<PrefetchTaskForBinding<O, AbortableIterator<O>>> prepareNextBatchExec(boolean isInNewThread) {
        GroupedBatch<G, Long, I> batchRequest = batchIterator.next();
        Batch<Long, I> batch = batchRequest.getBatch();
        NavigableMap<Long, I> batchItems = batch.getItems();

        G groupKey = batchRequest.getGroupKey();
        List<I> inputs = List.copyOf(batchItems.values());
        List<Long> reverseMap = List.copyOf(batchItems.keySet());

        IteratorCreator<O> creator = processBatch(isInNewThread, groupKey, inputs, reverseMap);

        InstanceLifeCycle<PrefetchTaskForBinding<O, AbortableIterator<O>>> result = InstanceLifeCycles.of(() -> {
            creator.begin();
            AbortableIterator<O> tmp = creator.createIterator();
            PrefetchTaskForBinding<O, AbortableIterator<O>> task = new PrefetchTaskForBinding<>(tmp, concurrentSlotReadAheadCount, reverseMap, this::copy);
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
        activeIter.close();

        for (TaskEntry<O, AbortableIterator<O>> taskEntry : inputToOutputIt.values()) {
            Closeable closable = taskEntry.stopAndGet();
            closable.close();
        }

//      if (executorServicePool != null) {
//          executorServicePool.shutdownAll();
//      }

        batchIterator.close();
    }

    @Override
    protected void closeIterator() {
        freeResources();
        super.closeIterator();
    }
}
