package org.apache.jena.sparql.service.enhancer.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.service.enhancer.impl.RequestExecutorBase.Granularity;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterator;
import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterators;
import org.junit.Test;

public class TestRequestExecutorBase {
	@Test
	public void test() {
		int outer = 100000;
		int inner = 100;

		int taskSlots = 20;
		int readAhead = 100000;

		Iterator<Entry<String, String>> plainIt = IntStream.range(9, outer).boxed()
			.flatMap(i -> IntStream.range(0, inner).mapToObj(j -> Map.entry("group" + i, "item" + j)))
			.iterator();

		Batcher<String, Entry<String, String>> batcher = new Batcher<>(Entry::getKey, 3, 10);

		AbortableIterator<GroupedBatch<String, Long, Entry<String, String>>> batchedIt = batcher.batch(AbortableIterators.wrap(plainIt));

		RequestExecutorBase<String, Entry<String, String>, Entry<Long, String>> executor = new RequestExecutorBase<>(
				new AtomicBoolean(),
				Granularity.ITEM,
				batchedIt,
				taskSlots,
				readAhead) {

					@Override
					public void output(IndentedWriter out, SerializationContext sCxt) {
					}

					@Override
					protected boolean isCancelled() {
						return false;
					}

					@Override
					protected IteratorCreator<Entry<Long, String>> processBatch(boolean isInNewThread, String groupKey,
							List<Entry<String, String>> batch, List<Long> reverseMap) {
						return () -> AbortableIterators.wrap(IntStream.range(0,  batch.size())
								.mapToObj(i -> Map.entry(reverseMap.get(i), batch.get(i).getValue())).iterator());
					}

					@Override
					protected long extractInputOrdinal(Entry<Long, String> input) {
						return input.getKey();
					}

					@Override
					protected void checkCanExecInNewThread() {
					}
			};


		while (executor.hasNext()) {
			Entry<Long, String> item = executor.next();
			System.out.println(item);
		}

		executor.close();
	}
}
