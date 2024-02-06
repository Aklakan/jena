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

package org.apache.jena.sparql.service.enhancer.claimingcache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.service.enhancer.impl.util.LockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;


/**
 * Implementation of async claiming cache.
 * Claimed entries will never be evicted. Conversely, unclaimed items remain are added to a cache such that timely re-claiming
 * will be fast.
 *
 * Use cases:
 * - Resource sharing: Ensure that the same resource is handed to all clients requesting one by key.
 * - Resource pooling: Claimed resources will never be closed, but unclaimed resources (e.g. something backed by an input stream)
 *   may remain on standby for a while.
 *
 * Another way to view this class is as a mix of a map with weak values and a cache.
 *
 * @author raven
 *
 * @param <K>
 * @param <V>
 */
public class AsyncClaimingCacheImplCaffeine<K, V>
    implements AsyncClaimingCache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncClaimingCacheImplCaffeine.class);

    // level1: claimed items - those items will never be evicted as long as the references are not closed
    protected Map<K, RefFuture<V>> level1;

    // level2: the caffine cache - items in this cache are not claimed are subject to eviction according to configuration
    protected AsyncCache<K, V> level2;
    protected Function<K, CompletableFuture<V>> level3AwareCacheLoader;

    // level3: items evicted from level2 but caught be eviction protection
    protected Map<K, V> level3;

    // Runs atomically in the claim action after the entry exists in level1
    protected BiConsumer<K, RefFuture<V>> claimListener;

    // Runs atomically in the unclaim action before the entry is removed from level1
    protected BiConsumer<K, RefFuture<V>> unclaimListener;

    // A lock that prevents invalidation while entries are being loaded
    protected ReentrantReadWriteLock invalidationLock = new ReentrantReadWriteLock();

    // A list of predicates that decide whether a key is considered protected from eviction
    // Each predicate abstracts matching a set of keys, e.g. a range of integer values.
    // The predicates are assumed to always return the same result for the same argument.
    protected final Collection<Predicate<? super K>> evictionGuards;

    protected RemovalListener<K, V> evictionListener;

    protected Set<K> suppressedRemovalEvents;

    public AsyncClaimingCacheImplCaffeine(
            Map<K, RefFuture<V>> level1,
            AsyncCache<K, V> level2,
            Function<K, V> level3AwareCacheLoader,
            Map<K, V> level3,
            Collection<Predicate<? super K>> evictionGuards,
            BiConsumer<K, RefFuture<V>> claimListener,
            BiConsumer<K, RefFuture<V>> unclaimListener,
            RemovalListener<K, V> evictionListener,
            Set<K> suppressedRemovalEvents
            ) {
        super();
        this.level1 = level1;
        this.level2 = level2;
        this.level3AwareCacheLoader = k -> CompletableFuture.completedFuture(level3AwareCacheLoader.apply(k));
        this.level3 = level3;
        this.evictionGuards = evictionGuards;
        this.claimListener = claimListener;
        this.unclaimListener = unclaimListener;
        this.evictionListener = evictionListener;
        this.suppressedRemovalEvents = suppressedRemovalEvents;
    }

    @Override
    public void cleanUp() {
        level2.synchronous().cleanUp();
    }

    protected Map<K, Latch> keyToSynchronizer = new ConcurrentHashMap<>();

    /**
     * Registers a predicate that 'caches' entries about to be evicted
     * When closing the registration then keys that have not moved back into the cache
     * by reference will be immediately evicted.
     */
    @Override
    public Closeable addEvictionGuard(Predicate<? super K> predicate) {
        // Note: LinkedList.listIterator() becomes invalidated after any modification
        // In principle a LinkedList would be the more appropriate data structure
        synchronized (evictionGuards) {
            evictionGuards.add(predicate);
        }
        return () -> {
            synchronized (evictionGuards) {
                evictionGuards.remove(predicate);
                runLevel3Eviction();
            }
        };
    }

    /** Called while being synchronized on the evictionGuards */
    protected void runLevel3Eviction() {
        Iterator<Entry<K, V>> it = level3.entrySet().iterator();
        while (it.hasNext()) {
            Entry<K, V> e = it.next();
            K k = e.getKey();
            V v = e.getValue();

            boolean isGuarded = evictionGuards.stream().anyMatch(p -> p.test(k));
            if (!isGuarded) {
                evictionListener.onRemoval(k, v, RemovalCause.COLLECTED);
                it.remove();
            }
        }
    }

    @Override
    public RefFuture<V> claim(K key) {
        RefFuture<V> result;

        // We rely on ConcurrentHashMap.compute operating atomically
        Latch synchronizer = keyToSynchronizer.compute(key, (k, before) -> before == null ? new Latch() : before.inc());

        // XXX In principle, we could move to synchronizedMap.compute for (approximately) per-key synchronization.
        //   The issue is, that RefImpl currently uses synchronized (synchronizer) { } blocks.
        //   This could be abstracted as a lambda that runs actions in either in a synchronized block or
        //   a synchronizedMap.compute function.

        // /guarded_entry/ marker; referenced in comment below

        synchronized (synchronizer) {
            keyToSynchronizer.compute(key, (k, before) -> before.dec());
            boolean[] isFreshSecondaryRef = { false };

            // Guard against concurrent invalidations
            RefFuture<V> secondaryRef = LockUtils.runWithLock(invalidationLock.readLock(), () -> {
                return level1.computeIfAbsent(key, k -> {
                    // Wrap the loaded reference such that closing the fully loaded reference adds it to level 2

                    if (logger.isTraceEnabled()) {
                        logger.trace("Claiming item [" + key + "] from level2");
                    }
                    CompletableFuture<V> future;
//                    try {
                        future = level2.get(key, (kkk, executor) -> level3AwareCacheLoader.apply(kkk));
//                    } catch (ExecutionException e) {
//                        throw new RuntimeException("Should not happen", e);
//                    }

                    // level2.invalidate(key) triggers level2's removal listener but we are about to add the item to level1
                    // so we don't want to publish a removal event to the outside
                    suppressedRemovalEvents.add(key);
                    level2.synchronous().invalidate(key);
                    suppressedRemovalEvents.remove(key);

                    @SuppressWarnings("unchecked")
                    RefFuture<V>[] holder = new RefFuture[] {null};

                    Ref<CompletableFuture<V>> freshSecondaryRef =
                        RefImpl.create(future, synchronizer, () -> {

                            // This is the unclaim action

                            RefFuture<V> v = holder[0];

                            if (unclaimListener != null) {
                                unclaimListener.accept(key, v);
                            }

                            RefFutureImpl.cancelFutureOrCloseValue(future, null);
                            level1.remove(key);
                            if (logger.isTraceEnabled()) {
                                logger.trace("Item [" + key + "] was unclaimed. Transferring to level2.");
                            }
                            level2.put(key, future);

                            // If there are no waiting threads we can remove the latch
                            keyToSynchronizer.compute(key, (kk, before) -> before.get() == 0 ? null : before);
                            // syncRef.close();
                        });
                    isFreshSecondaryRef[0] = true;

                    RefFuture<V> r = RefFutureImpl.wrap(freshSecondaryRef);
                    holder[0] = r;

                    return r;
                });
            });

            result = secondaryRef.acquire();

            if (claimListener != null) {
                claimListener.accept(key, result);
            }

            if (isFreshSecondaryRef[0]) {
                secondaryRef.close();
            }
        }
        return result;
    }

    // @Override
    public RefFuture<V> claimOld(K key) {
        RefFuture<V> result;

        // We rely on ConcurrentHashMap.compute operating atomically
        Latch synchronizer = keyToSynchronizer.compute(key, (k, before) -> before == null ? new Latch() : before.inc());

        // XXX In principle, we could move to synchronizedMap.compute for (approximately) per-key synchronization.
        //   The issue is, that RefImpl currently uses synchronized (synchronizer) { } blocks.
        //   This could be abstracted as a lambda that runs actions in either in a synchronized block or
        //   a synchronizedMap.compute function.

        // /guarded_entry/ marker; referenced in comment below

        synchronized (synchronizer) {
            keyToSynchronizer.compute(key, (k, before) -> before.dec());
            boolean[] isFreshSecondaryRef = { false };

            // Guard against concurrent invalidations
            RefFuture<V> secondaryRef = LockUtils.runWithLock(invalidationLock.readLock(), () -> {
                return level1.computeIfAbsent(key, k -> {
                    // Wrap the loaded reference such that closing the fully loaded reference adds it to level 2

                    logger.trace("Claiming item [" + key + "] from level2");
                    CompletableFuture<V> future = level2.get(key, (kkk, exec) -> null /* FIXME */);
                    level2.asMap().remove(key);


                    @SuppressWarnings("unchecked")
                    RefFuture<V>[] holder = new RefFuture[] {null};


                    Ref<CompletableFuture<V>> freshSecondaryRef =
                        RefImpl.create(future, synchronizer, () -> {

                            // This is the unclaim action

                            RefFuture<V> v = holder[0];

                            if (unclaimListener != null) {
                                unclaimListener.accept(key, v);
                            }

                            RefFutureImpl.cancelFutureOrCloseValue(future, null);
                            level1.remove(key);
                            logger.trace("Item [" + key + "] was unclaimed. Transferring to level2.");
                            level2.put(key, future);

                            // If there are no waiting threads we can remove the latch
                            keyToSynchronizer.compute(key, (kk, before) -> before.get() == 0 ? null : before);
                        });
                    isFreshSecondaryRef[0] = true;

                    RefFuture<V> r = RefFutureImpl.wrap(freshSecondaryRef);
                    holder[0] = r;

                    return r;
                });
            });

            result = secondaryRef.acquire();

            if (claimListener != null) {
                claimListener.accept(key, result);
            }

            if (isFreshSecondaryRef[0]) {
                secondaryRef.close();
            }
        }

        return result;
    }

    public static class Builder<K, V>
    {
        protected Caffeine<Object, Object> caffeine;
        protected CacheLoader<K, V> cacheLoader;
        protected BiConsumer<K, RefFuture<V>> claimListener;
        protected BiConsumer<K, RefFuture<V>> unclaimListener;
        protected RemovalListener<K, V> evictionListener;

        Builder<K, V> setCaffeine(Caffeine<Object, Object> caffeine) {
            this.caffeine = caffeine;
            return this;
        }

        public Builder<K, V> setClaimListener(BiConsumer<K, RefFuture<V>> claimListener) {
            this.claimListener = claimListener;
            return this;
        }

        public Builder<K, V> setUnclaimListener(BiConsumer<K, RefFuture<V>> unclaimListener) {
            this.unclaimListener = unclaimListener;
            return this;
        }

        public Builder<K, V> setCacheLoader(CacheLoader<K, V> cacheLoader) {
            this.cacheLoader = cacheLoader;
            return this;
        }

        public Builder<K, V> setEvictionListener(RemovalListener<K, V> evictionListener) {
            this.evictionListener = evictionListener;
            return this;
        }

        @SuppressWarnings("unchecked")
        public AsyncClaimingCacheImplCaffeine<K, V> build() {

            Map<K, RefFuture<V>> level1 = new ConcurrentHashMap<>();
            Map<K, V> level3 = new ConcurrentHashMap<>();
            Collection<Predicate<? super K>> evictionGuards = new ArrayList<>();

            RemovalListener<K, V> level3AwareAtomicRemovalListener = (k, v, c) -> {
                // Check for actual removal - key no longer present in level1
                if (!level1.containsKey(k)) {

                    boolean isGuarded = false;
                    synchronized (evictionGuards) {
                        // Check for an eviction guard
                        for (Predicate<? super K> evictionGuard : evictionGuards) {
                            isGuarded = evictionGuard.test(k);
                            if (isGuarded) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Protecting from eviction: " + k + " - " + level3.size() + " items protected");
                                }
                                level3.put(k, v);
                                break;
                            }
                        }
                    }

                    if (!isGuarded) {
                        if (evictionListener != null) {
                            evictionListener.onRemoval(k, v, c);
                        }
                    }
                }
            };

            Set<K> suppressedRemovalEvents = Collections.newSetFromMap(new ConcurrentHashMap<>());

            // Caffeine<Object, Object> finalLevel2Builder = caffeine.removalListener((k, v, c) -> {
            Caffeine<Object, Object> finalLevel2Builder = caffeine.evictionListener((k, v, c) -> {
                K kk = (K)k;
                boolean isEventSuppressed = suppressedRemovalEvents.contains(kk);
                if (!isEventSuppressed) {
                    // CompletableFuture<V> cfv = (CompletableFuture<V>)v;
                    CompletableFuture<V> cfv = CompletableFuture.completedFuture((V)v);

                    V vv = null;
                    if (cfv.isDone()) {
                        try {
                            vv = cfv.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException("Should not happen", e);
                        }
                    }

                    level3AwareAtomicRemovalListener.onRemoval(kk, vv, c);
                }
            });

//
//            caffeine.evictionListener((k, v, c) -> {
//                K kk = (K)k;
//                V vv = (V)v;
//
//                // Check for actual removal - key no longer present in level1
//                if (!level1.containsKey(k)) {
//                    level3AwareEvictionListener.onRemoval(kk, vv, c);
//                }
//            });


            // Cache loader that checks for existing items in level
            Function<K, V> level3AwareCacheLoader = k -> {
                Object[] tmp = new Object[] { null };
                // Atomically get and remove an existing key from level3
                level3.compute(k, (kk, v) -> {
                    tmp[0] = v;
                    return null;
                });

                V r = (V)tmp[0];
                if (r == null) {
                    try {
                        r = cacheLoader.load(k);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                return r;
            };

            AsyncCache<K, V> level2 = finalLevel2Builder.buildAsync();


            return new AsyncClaimingCacheImplCaffeine<>(level1, level2, level3AwareCacheLoader, level3, evictionGuards, claimListener, unclaimListener, level3AwareAtomicRemovalListener, suppressedRemovalEvents);
        }
    }

    public static <K, V> Builder<K, V> newBuilder(Caffeine<Object, Object> caffeine) {
        Builder<K, V> result = new Builder<>();
        result.setCaffeine(caffeine);
        return result;
    }


    public static void main(String[] args) throws InterruptedException {

        AsyncClaimingCacheImplCaffeine<String, String> cache = AsyncClaimingCacheImplCaffeine.<String, String>newBuilder(
                Caffeine.newBuilder().maximumSize(10).expireAfterWrite(1, TimeUnit.SECONDS).scheduler(Scheduler.systemScheduler()))
            .setCacheLoader(key -> "Loaded " + key)
            .setEvictionListener((k, v, c) -> System.out.println("Evicted " + k))
            .setClaimListener((k, v) -> System.out.println("Claimed: " + k))
            .setUnclaimListener((k, v) -> System.out.println("Unclaimed: " + k))
            .build();

        RefFuture<String> ref = cache.claim("hell");

        Closeable disposable = cache.addEvictionGuard(k -> k.contains("hell"));

        System.out.println(ref.await());
        ref.close();

        TimeUnit.SECONDS.sleep(5);

        RefFuture<String> reclaim = cache.claim("hell");

        disposable.close();

        reclaim.close();

        TimeUnit.SECONDS.sleep(5);

        System.out.println("done");
    }

    /**
     * Claim a key only if it is already present.
     *
     * This implementation is a best effort approach:
     * There is a very slim chance that just between testing a key for presence and claiming its entry
     * an eviction occurs - causing claiming of a non-present key and thus triggering a load action.
     */
    @Override
    public RefFuture<V> claimIfPresent(K key) {
        RefFuture<V> result = level1.containsKey(key) || level2.asMap().containsKey(key) ? claim(key) : null;
        return result;
    }


    @Override
    public void invalidateAll() {
        LockUtils.runWithLock(invalidationLock.writeLock(), () -> {
            level2.synchronous().invalidateAll();
            int present = level2.asMap().size();
            // int present = getPresentKeys().size();
            // if (present != 0) {
                System.out.println("Still present: " + present);
            // }
        });
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        LockUtils.runWithLock(invalidationLock.writeLock(), () -> {
            Map<K, CompletableFuture<V>> map = level2.asMap();
            for (K key : keys) {
                map.compute(key, (k, vFuture) -> {
                    V v = null;
                    if (v != null && vFuture.isDone()) {
                        try {
                            v = vFuture.get();
                        } catch (Exception e) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("Detected cache entry that failed to load during invalidation", e);
                            }
                        }
                        evictionListener.onRemoval(k, v, RemovalCause.EXPLICIT);
                    }
                    return null;
                });
            }
        });
    }

    /**
     * This method returns a snapshot of all keys across all internal cache levels.
     * It should only be used for informational purposes.
     */
    @Override
    public Collection<K> getPresentKeys() {
        Set<K> result = new LinkedHashSet<>();
        result.addAll(level1.keySet());
        result.addAll(level2.asMap().keySet());
        result.addAll(level3.keySet());
        return result;
    }

    /** Inner class use to synchronize per-key access - Essentially a 'NonAtomicInteger' */
    private static class Latch {
        // A flag to indicate that removal of the corresponding entry from keyToSynchronizer needs to be prevented
        // because another thread already started reusing this latch
        volatile int numWaitingThreads = 1;

        Latch inc() { ++numWaitingThreads; return this; }
        Latch dec() { --numWaitingThreads; return this; }
        int get() { return numWaitingThreads; }

        @Override
        public String toString() {
            return "Latch " + System.identityHashCode(this) + " has "+ numWaitingThreads + " threads waiting";
        }
    }
}
