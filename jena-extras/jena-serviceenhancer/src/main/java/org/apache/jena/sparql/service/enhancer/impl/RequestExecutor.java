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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.atlas.iterator.IteratorSlotted;
import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterConcat;
import org.apache.jena.sparql.engine.iterator.QueryIterConvert;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.iterator.QueryIteratorWrapper;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.service.enhancer.concurrent.ExecutorServicePool;
import org.apache.jena.sparql.service.enhancer.impl.util.BindingUtils;
import org.apache.jena.sparql.service.enhancer.impl.util.InstanceLifeCycle;
import org.apache.jena.sparql.service.enhancer.impl.util.InstanceLifeCycles;
import org.apache.jena.sparql.service.enhancer.impl.util.QueryIterSlottedBase;
import org.apache.jena.sparql.service.enhancer.impl.util.VarUtilsExtra;
import org.apache.jena.sparql.service.enhancer.init.ServiceEnhancerConstants;
import org.apache.jena.sparql.service.enhancer.init.ServiceEnhancerInit;
import org.apache.jena.system.TxnOp;

// FIXME This class is superseded by RequestExecutorBase/RequestExecutorBulkAndCache. It can be removed.

// FIXME This class has been generalized from forming bulk requests from a batch of input items
//       into one that can execute formed batches concurrently.
//       At the core, this is a flatMap operation.
//       The action to perform on a batch used to be hard coded, and now needs to go into an abstract method,
//       such as QueryIterator processBatch(List<Binding> batch).
//
// FIXME Add support for execution pooling - when a task completes than the thread that executed it needs to be
//       handed back to the pool.
//       The pool can be a global instance.

/**
 * Prepare and execute bulk requests.
 * Also allows prefetching of data using a number of concurrent slots.
 */
public class RequestExecutor
    extends QueryIterSlottedBase
{
    static class IteratorDelegateWithWorkerThread<T, I extends Iterator<T>>
        extends IteratorSlotted<T>
    {
        protected ExecutorServiceWrapperSync executorServiceSync;

        /** Number of items to transfer in batch from the worker thread to the calling thread */
        protected int batchSize;
        protected I delegate;

        protected volatile List<T> buffer;
        protected Iterator<T> currentBatch;

        /** Certain objects such as TDB2 Bindings must be copied in order to detach them from
         * resources that are free'd when the iterator is closed.
         */
        protected UnaryOperator<T> copyFn;

        public IteratorDelegateWithWorkerThread(I delegate, ExecutorService es, UnaryOperator<T> copyFn) {
            this(delegate, es, copyFn, 1);
        }

        public IteratorDelegateWithWorkerThread(I delegate, ExecutorService es, UnaryOperator<T> copyFn, int batchSize) {
            super();
            this.executorServiceSync = new ExecutorServiceWrapperSync(es);
            this.delegate = delegate;
            this.copyFn = copyFn;
            this.batchSize = batchSize;

            this.buffer = new ArrayList<>(batchSize);
            this.currentBatch = buffer.iterator();
        }

        public I getDelegate() {
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
                    I d = getDelegate();
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

        @Override
        protected boolean hasMore() {
            return true;
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
    }

    static class PrefetchTaskForBinding
        extends PrefetchTaskBase<Binding, QueryIterator> {

        /** The input ids in ascending order served by this task. Never empty. */
        protected List<Long> servedInputIds;

        /** The list item in {@link #servedInputIds}. */
        protected long closeInputId;

        public PrefetchTaskForBinding(QueryIterator iterator, long maxBufferedItemsCount, List<Long> servedInputIds, UnaryOperator<Binding> copyFn) {
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

        public static PrefetchTaskForBinding empty(ExecutionContext execCxt, long closeInputId) {
            return new PrefetchTaskForBinding(new QueryIterNullIterator(execCxt), 0, List.of(closeInputId), UnaryOperator.identity());
        }
    }

    /** Helper record for tracking running prefetch tasks. */
    static class TaskEntry {
        // protected LifeCycle<PrefetchTaskForBinding> taskLifeCycle;
        protected PrefetchTaskForBinding task;
        protected Closeable inThreadCloseAction;

        protected ExecutorService executorService;
        protected Closeable outThreadCloseAction;

        protected Future<?> future;
        protected ExecutionContext execCxt;

        protected volatile QueryIterPeek peekIter;

        public TaskEntry(PrefetchTaskForBinding task, Closeable inThreadCloseAction, ExecutorService executorService, Closeable outThreadCloseAction, Future<?> future, ExecutionContext execCxt) {
            super();
            this.task = task;
            this.inThreadCloseAction = inThreadCloseAction;
            this.executorService = executorService;
            this.outThreadCloseAction = outThreadCloseAction;
            this.future = future;
            this.execCxt = execCxt;
        }

        public ExecutionContext getExecCxt() {
            return execCxt;
        }

        public PrefetchTaskForBinding task() {
            return task;
        }

        public Future<?> future() {
            return future;
        }

        /**
         * Stop the task and return an iterator over the buffered items and the remaining ones.
         * Closing the returned iterator also closes the iterator over the remaining items
         */
        public QueryIterPeek stopAndGet() {
            if (peekIter == null) {
                // Send the abort signal
                task.stop();
                // Wait for the task to complete
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }

                QueryIterator tmp = task.getIterator();

                // If there is an executorService then make sure the iterator is accessed using it.
                if (executorService != null) {
                    IteratorCloseable<Binding> threadIt = new IteratorDelegateWithWorkerThread<>(tmp, executorService, task.getCopyFn()) {
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
                    tmp = QueryIterPlainWrapper.create(threadIt, execCxt);
                } else {
                    tmp = onClose(tmp, inThreadCloseAction);
                }

                // If there are buffered items then prepend them to 'tmp'
                List<Binding> bufferedItems = task.getBufferedItems();
                if (!bufferedItems.isEmpty()) {
                    QueryIterConcat concat = new QueryIterConcat(execCxt);
                    concat.add(QueryIterPlainWrapper.create(bufferedItems.iterator(), execCxt));
                    concat.add(tmp);
                    tmp = concat;
                }
                peekIter = QueryIterPeek.create(tmp, execCxt);
            }
            return peekIter;
        }

        public static TaskEntry completed(PrefetchTaskForBinding task, Closeable closeAction, ExecutionContext execCxt) {
            return new TaskEntry(
                task,
                closeAction,
                null,
                null,
                CompletableFuture.completedFuture(null),
                execCxt
            );
        }
        public static TaskEntry empty(ExecutionContext execCxt, long closeInputId) {
            return completed(PrefetchTaskForBinding.empty(execCxt, closeInputId), null, execCxt);
        }
    }

    protected OpServiceInfo serviceInfo;

    /**  Ensure that at least there are active requests to serve the next n input bindings */
    protected int fetchAhead = 5;
    protected int maxRequestSize = 2000;

    protected OpServiceExecutor opExecutor;
    protected ExecutionContext execCxt;
    protected ServiceResultSizeCache resultSizeCache;
    protected ServiceResponseCache cache;
    protected CacheMode cacheMode;

    protected IteratorCloseable<GroupedBatch<Node, Long, Binding>> batchIterator;
    protected Var globalIdxVar;

    // Input iteration
    protected long currentInputId = -1;
    protected TaskEntry activeTaskEntry = null;
    protected QueryIterPeek activeIter;

    protected Map<Long, TaskEntry> inputToOutputIt = new LinkedHashMap<>();

    /* State for tracking concurrent prefetch ----------------------------- */

    // private ExecutorService executorService = null;
    protected ExecutorServicePool executorServicePool;

    private final int maxConcurrentTasks;
    private final long concurrentSlotReadAheadCount;
    private final Map<Long, TaskEntry> openConcurrentTaskEntries = new LinkedHashMap<>();

    /* Actual implementation ---------------------------------------------- */

    public RequestExecutor(
            ExecutionContext execCxt,
            OpServiceExecutorImpl opExector,
            // boolean useLoopJoin,
            OpServiceInfo serviceInfo,
            ServiceResultSizeCache resultSizeCache,
            ServiceResponseCache cache,
            CacheMode cacheMode,
            IteratorCloseable<GroupedBatch<Node, Long, Binding>> batchIterator,
            // ExecutorService executorService,
            int maxConcurrentTasks,
            long concurrentSlotReadAheadCount) {
        super(execCxt);

        this.execCxt = execCxt;
        this.opExecutor = opExector;
        // this.useLoopJoin = useLoopJoin;
        this.serviceInfo = serviceInfo;
        this.resultSizeCache = resultSizeCache;
        this.cache = cache;
        this.cacheMode = cacheMode;
        this.batchIterator = batchIterator;

        // Allocate a fresh index var - services may be nested which results in
        // multiple injections of an idxVar which needs to be kept separate
        Set<Var> visibleServiceSubOpVars = serviceInfo.getVisibleSubOpVarsScoped();
        this.globalIdxVar = VarUtilsExtra.freshVar("__idx__", visibleServiceSubOpVars);

        // Set up a dummy task with an empty iterator as the active one and ensure it is properly closed
        activeTaskEntry = TaskEntry.empty(execCxt, currentInputId);
        inputToOutputIt.put(currentInputId, activeTaskEntry);

        this.activeIter = activeTaskEntry.stopAndGet();
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.concurrentSlotReadAheadCount = concurrentSlotReadAheadCount;

        this.executorServicePool = new ExecutorServicePool();
    }

    @Override
    protected Binding moveToNext() {

        Binding parentBinding = null;
        Binding childBindingWithIdx = null;

        // Peek the next binding on the active iterator and verify that it maps to the current
        // partition key
        while (!isCancelled(execCxt)) {
            if (activeIter.hasNext()) {
                Binding peek = activeIter.peek();
                // The iterator returns null if it was aborted
                if (peek == null) {
                    break;
                }

                long peekOutputId = BindingUtils.getNumber(peek, globalIdxVar).longValue();

                boolean inputIdMatches = peekOutputId == currentInputId;
                if (inputIdMatches) {
                    // parentBinding = inputToBinding.get(currentInputId);
                    childBindingWithIdx = activeIter.next();
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

        // Remove the idxVar from the childBinding
        Binding result = null;
        if (childBindingWithIdx != null) {
            Binding childBinding = BindingUtils.project(childBindingWithIdx, childBindingWithIdx.vars(), globalIdxVar);
            result = BindingFactory.builder(parentBinding).addAll(childBinding).build();
        }

        return result;
    }

    protected void registerTaskEntry(TaskEntry taskEntry) {
        List<Long> servedInputIds = taskEntry.task().getServedInputIds();
        for (Long e : servedInputIds) {
            inputToOutputIt.put(e, taskEntry);
        }
    }

    public void prepareNextBatchExecs() {
        TxnType txnType = null;

        if (!inputToOutputIt.containsKey(currentInputId)) {
            // We need the task's iterator right away - do not start concurrent retrieval
            if (batchIterator.hasNext()) {
                InstanceLifeCycle<PrefetchTaskForBinding> taskLifeCycle = prepareNextBatchExec(execCxt, null);
                PrefetchTaskForBinding task = taskLifeCycle.newInstance();
                TaskEntry taskEntry = TaskEntry.completed(task, () -> taskLifeCycle.closeInstance(task), execCxt);
                registerTaskEntry(taskEntry);
            }
        }

        DatasetGraph dataset = execCxt.getDataset();
        if (dataset.supportsTransactions()) {
            if (dataset.isInTransaction()) {
                ReadWrite txnMode = dataset.transactionMode();
                if (ReadWrite.WRITE.equals(txnMode)) {
                    throw new IllegalStateException("Cannot create concurrent tasks when in a write transaction.");
                }
                txnType = TxnType.READ;
            }
        }

        // Fill any remaining slots in the task queue for concurrent processing
        // Concurrent tasks have their own execution contexts because execCxt is not thread safe.
        while (openConcurrentTaskEntries.size() < maxConcurrentTasks && batchIterator.hasNext() && !isCancelled(execCxt)) {
            ExecutionContext isolatedExecCxt = new ExecutionContext(execCxt.getContext(), execCxt.getActiveGraph(), execCxt.getDataset(), execCxt.getExecutor());

            InstanceLifeCycle<PrefetchTaskForBinding> taskLifeCycle = prepareNextBatchExec(isolatedExecCxt, txnType);
            ExecutorService threadService = executorServicePool.acquireExecutor(); //.acquireExecutor(); // Executors.newSingleThreadExecutor();

            // Create the task through the execution thread such that thread locals (e.g. for transactions)
            // are initialized on the correct thread
            PrefetchTaskForBinding task = ExecutorServiceWrapperSync.submit(threadService, taskLifeCycle::newInstance);

            // Submit the task to the same thread
            Future<?> future = threadService.submit(task);
            TaskEntry taskEntry = new TaskEntry(task, () -> taskLifeCycle.closeInstance(task), threadService, () -> threadService.shutdown(), future, isolatedExecCxt);
            registerTaskEntry(taskEntry);
            openConcurrentTaskEntries.put(taskEntry.task().getCloseInputId(), taskEntry);
        }
    }

//    protected <T extends Transactional> InstanceLifeCycle<T> txnLifeCycle(Supplier<T> txnCreator, TxnType txnType) {
//        return InstanceLifeCycles.of(() -> {
//            T txn = txnCreator.get();
//            boolean b = txn.isInTransaction();
//            if ( b )
//                TxnOp.compatibleWithPromote(txnType, txn);
//            else
//                txn.begin(txnType);
//            return txn;
//        }, txn -> {
//        });
//    }

    private static void txnBegin(Transactional txn, TxnType txnType) {
        boolean b = txn.isInTransaction();
        if ( b )
            TxnOp.compatibleWithPromote(txnType, txn);
        else
            txn.begin(txnType);
    }

    private static void txnEnd(Transactional txn) {
        boolean b = txn.isInTransaction();
        if ( !b ) {
            if ( txn.isInTransaction() )
                // May have been explicit commit or abort.
                txn.commit();
            txn.end();
        }
    }


    /** Prepare the lazy execution of the next batch and register all iterators with {@link #inputToOutputIt} */
    // seqId = sequential number injected into the request
    // inputId = id (index) of the input binding
    // rangeId = id of the range w.r.t. to the input binding
    // partitionKey = (inputId, rangeId)
    protected InstanceLifeCycle<PrefetchTaskForBinding> prepareNextBatchExec(ExecutionContext batchExecCxt, TxnType txnType) {
        GroupedBatch<Node, Long, Binding> batchRequest = batchIterator.next();

        // TODO Support ServiceOpts from Node directly
        ServiceOpts so = ServiceOptsSE.getEffectiveService(serviceInfo.getOpService());

        Node targetServiceNode = so.getTargetService().getService();

        // Refine the request w.r.t. the cache
        Batch<Long, Binding> batch = batchRequest.getBatch();

        // This block sets up the execution of the batch
        // For aesthetics, bindings are re-numbered starting with 0 when creating the backend request
        // These ids are subsequently mapped back to the offset of the input iterator
        {
            NavigableMap<Long, Binding> batchItems = batch.getItems();

            List<Binding> inputs = new ArrayList<>(batchItems.values());

            NodeTransform serviceNodeRemapper = node -> ServiceEnhancerInit.resolveServiceNode(node, batchExecCxt);

            Set<Var> inputVarsMentioned = BindingUtils.varsMentioned(inputs);
            ServiceCacheKeyFactory cacheKeyFactory = ServiceCacheKeyFactory.createCacheKeyFactory(serviceInfo, inputVarsMentioned, serviceNodeRemapper);

            Set<Var> visibleServiceSubOpVars = serviceInfo.getVisibleSubOpVarsScoped();
            Var batchIdxVar = VarUtilsExtra.freshVar("__idx__", visibleServiceSubOpVars);

            BatchQueryRewriterBuilder builder = BatchQueryRewriterBuilder.from(serviceInfo, batchIdxVar);

            if (ServiceEnhancerConstants.SELF.equals(targetServiceNode)) {
                builder.setOrderRetainingUnion(true)
                    .setSequentialUnion(true);
            }

            BatchQueryRewriter rewriter = builder.build();

            DatasetGraph dsg = batchExecCxt.getDataset();

            InstanceLifeCycle<PrefetchTaskForBinding> result = InstanceLifeCycles.of(() -> {
                if (txnType != null) {
                    txnBegin(dsg, txnType);
                }

                QueryIterServiceBulk baseIt = new QueryIterServiceBulk(
                        serviceInfo, rewriter, cacheKeyFactory, opExecutor, batchExecCxt, inputs,
                        resultSizeCache, cache, cacheMode);

                QueryIterator tmp = baseIt;

                // Remap the local input id of the batch to the global one here
                Var innerIdxVar = baseIt.getIdxVar();
                List<Long> reverseMap = new ArrayList<>(batchItems.keySet());

                tmp = new QueryIterConvert(baseIt, b -> {
                    int localId = BindingUtils.getNumber(b, innerIdxVar).intValue();
                    long globalId = reverseMap.get(localId);

                    Binding q = BindingUtils.project(b, b.vars(), innerIdxVar);
                    Binding r = BindingFactory.binding(q, globalIdxVar, NodeValue.makeInteger(globalId).asNode());

                    return r;
                }, batchExecCxt);

                // XXX Ideally we'd only copy bindings when needed!
                PrefetchTaskForBinding task = new PrefetchTaskForBinding(tmp, concurrentSlotReadAheadCount, reverseMap, BindingFactory::copy);
                return task;
            }, task -> {
                try {
                    task.getIterator().close();
                } finally {
                    if (txnType != null) {
                        txnEnd(dsg);
                    }
                }
            });

            return result;
        }
    }

    protected void freeResources() {
        activeIter.close();

        for (TaskEntry taskEntry : inputToOutputIt.values()) {
            Closeable closable = taskEntry.stopAndGet();
            closable.close();
        }

        // All tasks are always also registered with inputToOutputId - so no need to iterate that map too.
        // for (TaskEntry taskEntry : openConcurrentTaskEntries.values()) {
        //    Closeable closable = taskEntry.stopAndGet();
        //    closable.close();
        // }

//        if (executorService != null) {
//            executorService.shutdown();
//            boolean isShutdown = false;
//            try {
//                isShutdown = executorService.awaitTermination(60, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//
//            if (!isShutdown) {
//                throw new IllegalStateException("Timeout waiting for executor service to shut down. This indicates a hanging task.");
//            }
//        }

        if (executorServicePool != null) {
            executorServicePool.shutdownAll();
        }

        batchIterator.close();
    }

    @Override
    protected void closeIteratorActual() {
        freeResources();
        // super.closeIterator();
    }

    @Override
    protected void requestCancel() {
    }

    private static boolean isCancelled(ExecutionContext execCxt) {
        AtomicBoolean ab = execCxt.getCancelSignal();
        return ab == null ? false : ab.get();
    }

    /** Wrap a given {@link QueryIterator} with an additional close action. */
    // XXX This static util method could be moved to QueryIter
    private static QueryIterator onClose(QueryIterator qIter, Closeable action) {
        Objects.requireNonNull(qIter);
        QueryIterator result = action == null
            ? qIter
            : new QueryIteratorWrapper(qIter) {
                @Override
                protected void closeIterator() {
                    try {
                        action.close();
                    } finally {
                        super.closeIterator();
                    }
                }
            };
        return result;
    }
}
