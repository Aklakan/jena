package org.apache.jena.sparql.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.locks.Lock;

import org.aksw.commons.io.slice.Slice;
import org.aksw.commons.io.slice.SliceAccessor;
import org.aksw.commons.util.ref.RefFuture;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.service.BatchQueryRewriter.BatchQueryRewriteResult;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.math.LongMath;

public class RequestExecutor {

	protected OpServiceInfo serviceInfo;

	/**  Ensure that at least there are active requests to serve the next n input bindings */
	protected int fetchAhead = 5;
	protected int maxRequestSize = 2000;

	protected Iterator<ServiceBatchRequest<Node, Binding>> batchIterator;
	protected SimpleServiceCache cache;

	protected long nextOutputId = 0;

	protected NavigableMap<Long, PartitionIterator> nextOutputIdToIterator;

	public RequestExecutor(OpServiceInfo serviceInfo, Iterator<ServiceBatchRequest<Node, Binding>> batchIterator) {
		this.serviceInfo = serviceInfo;
		this.batchIterator = batchIterator;
		this.cache = new SimpleServiceCache();
	}

	public void exec() {

		ServiceBatchRequest<Node, Binding> batchRequest = batchIterator.next();
		Node substServiceNode = batchRequest.getGroupKey();


		// Refine the request w.r.t. the cache
		Batch<Binding> batch = batchRequest.getBatch();
		Iterator<Binding> itBindings = batch.getItemRanges().values().stream().flatMap(List::stream).iterator();

		Var idxVar = Var.alloc("__idx__");
		BatchQueryRewriter rewriter = new BatchQueryRewriter(serviceInfo, idxVar);


		List<PartitionRequest<Binding>> backendRequests = new ArrayList<>();

		// List<CacheRequest<>>
		List<Object> cacheRequests = null;


		// long nextOutputId;
		while (itBindings.hasNext()) {
			Binding binding = itBindings.next();

			ServiceCacheKey cacheKey = new ServiceCacheKey(substServiceNode, serviceInfo.getRawQueryOp(), binding);
			RefFuture<ServiceCacheValue> cacheValueRef = cache.getCache().claim(cacheKey);
			ServiceCacheValue serviceCacheValue = cacheValueRef.await();

			Slice<Binding[]> slice = serviceCacheValue.getSlice();
			Lock lock = slice.getReadWriteLock().readLock();

			SliceAccessor<Binding[]> accessor = slice.newSliceAccessor();
			lock.lock();


			RangeSet<Long> loadedRanges;
			long knownSize;
			try {
				loadedRanges = slice.getLoadedRanges();
				knownSize = slice.getKnownSize();
				accessor.claimByOffsetRange(0, Long.MAX_VALUE);
			} finally {
				lock.unlock();
			}

			// Iterate the present/absent ranges
			long start = serviceInfo.getOffset();
			if (start == Query.NOLIMIT) {
				start = 0;
			}

			long limit = serviceInfo.getLimit();

			long max = knownSize < 0 ? Long.MAX_VALUE : knownSize;
			long end = limit == Query.NOLIMIT ? max : LongMath.saturatedAdd(start, limit);
			end = Math.min(end, max);


			Range<Long> initialRange = knownSize < 0
				? Range.atLeast(start)
				: Range.closedOpen(start, end);

			RangeSet<Long> missingRanges = loadedRanges.complement().subRangeSet(initialRange);

			RangeMap<Long, Boolean> allRanges = TreeRangeMap.create();
			loadedRanges.asRanges().forEach(r -> allRanges.put(r, true));
			missingRanges.asRanges().forEach(r -> allRanges.put(r, false));

			for (Entry<Range<Long>, Boolean> e : allRanges.asMapOfRanges().entrySet()) {
				Range<Long> range = e.getKey();
				boolean isLoaded = e.getValue();

				long lo = range.lowerEndpoint();
				long hi = range.hasUpperBound() ? range.upperEndpoint() : Long.MAX_VALUE;

				if (isLoaded) {
					PartitionRequest<Binding> request = new PartitionRequest<>(nextOutputId, binding, lo, hi);
					backendRequests.add(request);
				} else {
					// TODO Declare the current 'nextOutputId' to be served from the cache
					// TODO Claim the range the avoid eviction
					// ISSUE The cache currently does not support claiming without loading
					//   This can load to out-of-memory issues
				}
				++nextOutputId;
				// mgr.add(request);
			}
		}

		BatchQueryRewriteResult rewrite = rewriter.rewrite(backendRequests);
		System.out.println(rewrite);



//
//
//		// TODO Check if there is any scheduled request that would add more ranges to data
//
//
//		Query rawQuery = serviceInfo.getRawQuery();
//		Range<Long> range = RangeUtils.toRange(serviceInfo.getOffset(), serviceInfo.getLimit());
//		RangeSet<Long> fetchRanges = serviceCacheValue.getFetchRanges(range);
//
//		SingleServiceRequestMgr mgr = requestMgr.computeIfAbsent(substServiceNode, k -> new SingleServiceRequestMgr());
//



	}



}
