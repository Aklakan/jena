package org.apache.jena.sparql.service.enhancer.impl;

import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public class Meter {
	private Deque<Entry<Long, Long>> data;
	private long total = 0;
	private int maxDataSize;

	private long lastTick = -1;
	private AtomicLong counter = new AtomicLong();

	public Meter(int maxDataSize) {
		super();
		if (maxDataSize < 1) {
			throw new IllegalArgumentException("Data size must be at least 1.");
		}
		this.maxDataSize = maxDataSize;
	}

	public void inc() {
		counter.incrementAndGet();
	}

	public void tick() {
		long time = System.currentTimeMillis();
		long value = counter.getAndSet(0);

		if (data.size() >= maxDataSize) {
			Entry<Long, Long> e = data.removeFirst();
			total -= e.getValue();
		}

		total += value;
		data.add(Map.entry(time, value));
		lastTick = time;
	}
}
