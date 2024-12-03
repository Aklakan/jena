package org.apache.jena.sparql.service.enhancer.impl;

import java.util.function.Function;

import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterator;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterator1;

/** Simple batch that puts each item into its own group. */
public class BatcherSimple<G, I> {
    protected Function<I, G> itemToGroupKey;
    protected int batchSize;

    public BatcherSimple(Function<I, G> itemToGroupKey) {
        this.itemToGroupKey = itemToGroupKey;
    }

    public AbortableIterator<GroupedBatch<G, Long, I>> batch(AbortableIterator<I> inputIterator) {
        return new AbortableIterator1<>(inputIterator) {
            private long nextIndex = 0;

            @Override
            protected void requestSubCancel() {}

            @Override
            protected void closeSubIterator() {}

            @Override
            protected boolean hasNextBinding() {
                return inputIterator.hasNext();
            }

            @Override
            protected GroupedBatch<G, Long, I> moveToNextBinding() {
                I item = inputIterator.next();
                G groupKey = itemToGroupKey.apply(item);
                long idx = nextIndex++;
                Batch<Long, I> batch = BatchImpl.forLong(idx);
                batch.put(idx, item);
                return new GroupedBatch<>(groupKey, batch);
            }
        };
    }
}
