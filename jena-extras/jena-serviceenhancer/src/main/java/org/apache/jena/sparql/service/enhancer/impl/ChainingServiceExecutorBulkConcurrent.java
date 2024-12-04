package org.apache.jena.sparql.service.enhancer.impl;

import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.service.ServiceExec;
import org.apache.jena.sparql.service.bulk.ChainingServiceExecutorBulk;
import org.apache.jena.sparql.service.bulk.ServiceExecutorBulk;
import org.apache.jena.sparql.service.enhancer.impl.RequestExecutorBase.Granularity;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterator;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterators;
import org.apache.jena.sparql.service.enhancer.init.ServiceEnhancerConstants;
import org.apache.jena.sparql.util.Context;

public class ChainingServiceExecutorBulkConcurrent
    implements ChainingServiceExecutorBulk
{
    public static final String FEATURE_NAME = "concurrent";

    private final String name;

    public ChainingServiceExecutorBulkConcurrent() {
        this(FEATURE_NAME);
    }

    public ChainingServiceExecutorBulkConcurrent(String name) {
        super();
        this.name = name;
    }

    @Override
    public QueryIterator createExecution(OpService opService, QueryIterator input, ExecutionContext execCxt, ServiceExecutorBulk chain) {
//        ServiceOpts opts = ServiceOpts.getEffectiveService(opService, ServiceEnhancerConstants.SELF.getURI(),
//                key -> key.equals(name));
        List<Entry<String, String>> list = ServiceOpts.parseEntries(opService.getService());

        QueryIterator result;
        Entry<String, String> opt = list.isEmpty() ? null : list.get(0);
        if (opt != null && opt.getKey().equals(name)) {
            list = list.subList(1, list.size());
            // Remove a trailing colon separator
            // FIXME: This should be handled more elegantly
            if (!list.isEmpty() && list.get(0).getKey().equals("")) {
                list = list.subList(1, list.size());
            }

            OpService newOp = ChainingServiceExecutorBulkServiceEnhancer.toOpService(list, opService, ServiceEnhancerConstants.SELF_BULK);

            int concurrentSlots = 0;
            long readaheadOfBindingsPerSlot = ChainingServiceExecutorBulkCache.DFT_CONCURRENT_READAHEAD;

            Context cxt = execCxt.getContext();
            // String key = opt.getKey();
            String val = opt.getValue();

            int maxConcurrentSlotCount = cxt.get(ServiceEnhancerConstants.serviceConcurrentMaxSlotCount, ChainingServiceExecutorBulkCache.DFT_MAX_CONCURRENT_SLOTS);
            // Value pattern is: [concurrentSlots][-maxReadaheadOfBindingsPerSlot]
            String v = val == null ? "" : val.toLowerCase().trim();
            int bindingsPerSlot = 1;

            // [{concurrentSlotCount}[-{bindingsPerSlotCount}[-{readAheadPerSlotCount}]]]
            if (!v.isEmpty()) {
                String[] parts = v.split("-", 3);
                if (parts.length > 0) {
                    concurrentSlots = parseInt(parts[0], 0);
                    if (parts.length > 1) {
                        bindingsPerSlot = parseInt(parts[1], 0);
                        // There must be at least 1 binding per slot
                        bindingsPerSlot = Math.max(1, bindingsPerSlot);
                        if (parts.length > 2) {
                            int maxReadaheadOfBindingsPerSlot = cxt.get(ServiceEnhancerConstants.serviceConcurrentMaxReadaheadCount, ChainingServiceExecutorBulkCache.DFT_MAX_CONCURRENT_READAHEAD);
                            readaheadOfBindingsPerSlot = parseInt(parts[2], 0);
                            readaheadOfBindingsPerSlot = Math.max(Math.min(readaheadOfBindingsPerSlot, maxReadaheadOfBindingsPerSlot), 0);
                        }
                    }
                }
            } else {
                concurrentSlots = Runtime.getRuntime().availableProcessors();
            }
            concurrentSlots = Math.max(Math.min(concurrentSlots, maxConcurrentSlotCount), 0);

            // OpServiceInfo serviceInfo = new OpServiceInfo(opService);
            Node serviceNode = opService.getService();
            // OpServiceInfo serviceInfo = new OpServiceInfo(newOp);
            Function<Binding, Node> groupKeyFn = binding -> Var.lookup(binding, serviceNode);
            // Function<Binding, Node> groupKeyFn = serviceInfo::getSubstServiceNode;

            Batcher<Node, Binding> scheduler = new Batcher<>(groupKeyFn, bindingsPerSlot, 0);
            AbortableIterator<GroupedBatch<Node, Long, Binding>> inputBatchIterator = scheduler.batch(AbortableIterators.adapt(input));

            RequestExecutorJenaBase exec = new RequestExecutorJenaBase(Granularity.BATCH, inputBatchIterator, concurrentSlots, readaheadOfBindingsPerSlot, execCxt) {
                @Override
                protected AbortableIterator<Binding> buildIterator(boolean runsOnNewThread, Node groupKey, List<Binding> inputs, List<Long> reverseMap, ExecutionContext batchExecCxt) {
//                    Iterator<Binding> indexedBindings = IntStream.range(0, inputs.size()).mapToObj(i ->
//                        BindingFactory.binding(inputs.get(i), globalIdxVar, NodeValue.makeInteger(reverseMap.get(i)).asNode()))
//                        .iterator();

                    QueryIterator subIter = QueryIterPlainWrapper.create(inputs.iterator(), execCxt);

                    // QueryIterator tmp = chain.createExecution(newOp, QueryIterPlainWrapper.create(indexedBindings, execCxt), execCxt);
                    // Pass the adapted request through the whole service executor chain again.
                    QueryIterator tmp = ServiceExec.exec(subIter, newOp, execCxt);
                    return AbortableIterators.adapt(tmp);
                }

                @Override
                protected long extractLocalInputId(Binding input) {
                    throw new IllegalStateException("Should never be called.");
                }
            };
            result = AbortableIterators.asQueryIterator(exec);
        } else {
            result = chain.createExecution(opService, input, execCxt);
        }
        return result;
    }

    private int parseInt(String str, int fallbackValue) {
        return str.isEmpty() ? fallbackValue : Integer.parseInt(str);
    }
}
