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

package org.apache.jena.riot.system;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.lib.Trie;
import org.apache.jena.ext.com.google.common.base.Preconditions;
import org.apache.jena.ext.com.google.common.cache.Cache;
import org.apache.jena.ext.com.google.common.cache.CacheBuilder;
import org.apache.jena.ext.com.google.common.collect.Multimap;
import org.apache.jena.ext.com.google.common.collect.Multimaps;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.graph.PrefixMappingBase;

import com.github.jsonldjava.shaded.com.google.common.collect.Iterables;

/**
 * In-memory implementation of a {@link PrefixMap}.
 * <p>
 * This also provides fast URI to prefix name calculation suitable for output. For
 * output, calculating possible prefix names from a URI happens on every URI so this
 * operations needs to be efficient. Normally, a prefix map is "prefix to URI" and
 * the abbreviation is a reverse lookup, which is a scan of the value of the map.
 * This class keeps a reverse lookup map of URI to prefix which combined with a fast,
 * approximate for determining the split point exploiting the most common use cases,
 * provides efficient abbreviation.
 * <p>
 * Usage for abbreviation: call
 * {@linkplain PrefixMapFactory#createForOutput(PrefixMap)} which copies the argument
 * prefix map into an instance of this class, setting up the reverse lookup. This
 * copy is cheaper than repeated reverse lookups would be.
 */
public class PrefixMapStd extends PrefixMapBase implements Serializable {
    private static final long serialVersionUID = 1L;

    // See the setters of PrefixMapBuilder for explanations of these constants

    public static final int DFT_CACHE_SIZE = 1000;
    public static final boolean DFT_DELTA_ENABLED = true;
    public static final boolean DFT_FAST_TRACK_ENABLED = true;
    public static final char[] DFT_FAST_TRACK_SEPARATORS = new char[]{'#', '/', ':'};
    public static final boolean DFT_TRIE_ENABLED = true;
    public static final boolean DFT_EAGER_IRI_TO_PREFIX = true;
    public static final boolean DFT_LAST_IRI_WINS = true;
    public static final boolean DFT_INIT_TRIE_LAZILY = true;
    public static final boolean DFT_LOCKING_ENABLED = true;
    public static final Supplier<Map<String, String>> DFT_MAP_FACTORY = ConcurrentHashMap::new;

    /** Prefix-to-iri mapping. never null. */
    private final Map<String, String> prefixToIri;
    private final Map<String, String> prefixToIriView;

    /** The set of entries that have not yet been indexed for reverse lookup */
    private Map<String, Optional<String>> delta;

    /** The map used for fast track lookups.
     * For exact matches of IRI namespaces the map is much faster than the trie. */
    private Multimap<String, String> iriToPrefixes = null;

    /** Trie for longest prefix lookups; may be initialized lazily when needed */
    private Trie<String> iriToPrefixTrie = null;

    /** Cache for mapping iris to prefixes.
     * Wrapping with Optional is needed because the Guava Cache does not allow for null values */
    private final Cache<String, Optional<String>> cache;

    /** Disabling fast track always resorts to longest prefix lookup */
    private final boolean enableFastTrack;

    /** Enable trie-based lookups */
    private final boolean enableTrie;

    /** If override is enabled then after "add(p1, iri); add(p2, iri);" iri will map to p2; p1 otherwise. */
    private final boolean useLastPrefixForIri;

    /** Locking is optional */
    private transient final ReadWriteLock rwl;

    private final char[] fastTrackSeparators;

    /** A generation counter that is incremented on modifications and which is
     * used to invalidate the internal cache when needed.
     * If generation and cacheVersion differ then the next prefix lookup will invalidate the cache and
    /* set cacheVersion to generation */
    private int generation = 0;
    private int cacheVersion = 0;

    private int deltaSizeContrib = 0;

    public PrefixMapStd() {
        this(DFT_CACHE_SIZE);
    }

    /** Sets up a default PrefixMapStd and copies the prefixes from the argument. */
    public PrefixMapStd(PrefixMap prefixMap) {
        this(DFT_CACHE_SIZE);
        putAll(prefixMap);
    }

    public PrefixMapStd(long cacheSize) {
        this(DFT_MAP_FACTORY.get(), DFT_DELTA_ENABLED, cacheSize, DFT_FAST_TRACK_ENABLED, DFT_FAST_TRACK_SEPARATORS,
                DFT_TRIE_ENABLED, DFT_EAGER_IRI_TO_PREFIX, DFT_LAST_IRI_WINS, DFT_INIT_TRIE_LAZILY, DFT_LOCKING_ENABLED);
    }

    private void initIriToPrefixMap() {
        iriToPrefixes = Multimaps.newListMultimap(new HashMap<>(), () -> new ArrayList<>(1));
        // iriToPrefixMap.compute(iri, (i, oldPrefix) -> useLastPrefixForIri || oldPrefix == null ? prefix : oldPrefix)
        prefixToIri.forEach((prefix, iri) -> iriToPrefixes.put(iri, prefix));
    }

//    private static <K, V> V put(Map<K, V> map, K key, V value, boolean useLast) {
//        V[] result = (V[])new Object[] { null };
//        map.compute(key, (k, oldValue) -> (result[0] = oldValue) == null || useLast ? value : oldValue);
//        return result[0];
//    }

    /** Initializes the trie data structure for faster (non-fast-track) iri-to-prefix lookup. */
    private void initIriToPrefixLookup() {
        if (iriToPrefixes == null) {
            initIriToPrefixMap();
        }

        iriToPrefixTrie = new Trie<>();
        iriToPrefixes.forEach(iriToPrefixTrie::add);
    }

    /**
     * Create a PrefixMapStd instance using the the specified prefix-to-iri map implementation and cache size.
     * @param prefixToIri An empty map into which to store prefixes. Should not be changed externally.
     * @param cacheSize The cache size for prefix lookups.
     */
    PrefixMapStd(Map<String, String> prefixToIri, boolean useDelta, long cacheSize, boolean enableFastTrack, char[] fastTrackSeparators, boolean enableTrie, boolean eagerIriToPrefix , boolean addOverridesIriToPrefix, boolean initTrieLazily, boolean useLocking) {
        super();
        Objects.requireNonNull(prefixToIri);
        Preconditions.checkArgument(cacheSize >= 0, "Cache size must be non-negative");
        this.delta = useDelta ? new LinkedHashMap<>() : null;
        this.prefixToIri = prefixToIri;
        if (!prefixToIri.isEmpty()) {
            // Best effort check; the caller may still perform concurrent modifications to the supplied map
            throw new IllegalArgumentException("PrefixToIri map must be initially empty");
        }
        this.prefixToIriView = Collections.unmodifiableMap(prefixToIri);
        this.cache = cacheSize == 0
                ? null
                : CacheBuilder.newBuilder().maximumSize(cacheSize).build();
        this.enableFastTrack = enableFastTrack;
        this.fastTrackSeparators = fastTrackSeparators;
        this.enableTrie = enableTrie;
        this.useLastPrefixForIri = addOverridesIriToPrefix;
        if (eagerIriToPrefix) {
            initIriToPrefixMap();
        }
        if (!initTrieLazily) {
            initIriToPrefixLookup();
        }
        this.rwl = useLocking ? new ReentrantReadWriteLock() : null;
    }

    private Lock readLock() {
        return rwl == null ? null : rwl.readLock();
    }

    private Lock writeLock() {
        return rwl == null ? null : rwl.writeLock();
    }

    /** If the iri is already mapped to a different prefix then depending on the setup either the first or last one is retained */
    @Override
    public void add(String prefix, String iri) {
        if (rwl == null) {
            nonLockingAdd(prefix, iri);
        } else {
            execute(rwl.writeLock(), () -> {
                nonLockingAdd(prefix, iri);
            });
        }
    }

    /** Internal, non-locking "add" method.
     * This method is called repeatedly during bulk inserts. For this reason arguments are also checked. */
    private void nonLockingAdd(String prefix, String iri) {
        Objects.requireNonNull(prefix);
        Objects.requireNonNull(iri);
        String canonicalPrefix = PrefixLib.canonicalPrefix(prefix);

        if (delta != null) {
            delta.compute(canonicalPrefix, (p, oldIriOpt) -> {
                Optional<String> r = Optional.of(iri);
                if (!r.equals(oldIriOpt)) {
                    // Check if the entry being added already exists in the base map
                    String before = prefixToIri.get(p);
                    if (before != null) {
                        if (iri.equals(before)) {
                            r = null;
                            if (oldIriOpt != null && oldIriOpt.isPresent()) {
                                // If we would update an existing mapping in delta to equal one
                                // present in the base map then we can remove it from delta.
                                --deltaSizeContrib;
                            }
                        }
                    } else { // no prior entry
                        if (oldIriOpt == null || oldIriOpt.isEmpty()) {
                            ++deltaSizeContrib;
                        }
                    }
                } else {
                    r = oldIriOpt; // Return the old reference
                }
                return r;
            });
        } else {
            materializingAdd(canonicalPrefix, iri);
        }
    }

    /** Insert data into the non-delta structures */
    private void materializingAdd(String canonicalPrefix, String iri) {
        String oldIri = prefixToIri.put(canonicalPrefix, iri);
        if (!Objects.equals(oldIri, iri)) {
            // If there was a non-null oldIri and we are in override mode then remove oldIri
            if (oldIri != null && useLastPrefixForIri) {
                if (iriToPrefixes != null) { iriToPrefixes.remove(oldIri, canonicalPrefix); }
                if (iriToPrefixTrie != null) { iriToPrefixTrie.remove(oldIri); }
            }

            // If in override mode or if the iri was not mapped to a prefix then update
            if (useLastPrefixForIri || (iriToPrefixes != null && iriToPrefixes.get(iri) == null)) {
                if (iriToPrefixes != null) { iriToPrefixes.put(iri, canonicalPrefix); }
                if (iriToPrefixTrie != null) { iriToPrefixTrie.add(iri, canonicalPrefix); }
            }
            ++generation;
        }
    }

    /** See notes on reverse mappings in {@link PrefixMappingBase}.
     * This is a complete implementation.
     * <p>
     * Test {@code AbstractTestPrefixMapping.testSecondPrefixDeletedUncoversPreviousMap}.
     */
    @Override
    public void delete(String prefix) {
        if (rwl == null) {
            nonLockingDelete(prefix);
        } else {
            execute(rwl.writeLock(), () -> {
                nonLockingDelete(prefix);
            });
        }
    }

    private void nonLockingDelete(String prefix) {
        Objects.requireNonNull(prefix);
        String canonicalPrefix = PrefixLib.canonicalPrefix(prefix);
        if (delta != null) {
            delta.compute(canonicalPrefix, (p, oldIriOpt) -> {
                // If we are overriding a materialized entry then map to an empty optional,
                // otherwise just delete the delta entry.
                Optional<String> r = oldIriOpt;
                if (oldIriOpt == null || !oldIriOpt.isEmpty()) { // If not already marked as deleted
                    boolean existsMaterializedIri = prefixToIri.containsKey(prefix);
                    if (existsMaterializedIri) {
                        --deltaSizeContrib;
                        r = Optional.empty();
                    } else {
                        // Nothing to delete
                        r = null;
                    }
                }
                return r;
            });
        } else {
            materializingDelete(canonicalPrefix);
        }
    }

    /** Delete data from the non-delta structures */
    private void materializingDelete(String canonicalPrefix) {
        // Removal returns the previous value or null if there was none
        String iriForPrefix = prefixToIri.remove(canonicalPrefix);
        if (iriForPrefix != null) {
            if (iriToPrefixes != null) {
                Collection<String> prefixesForIri = iriToPrefixes.get(iriForPrefix);
                String activePrefix = getPrefix(prefixesForIri);
                prefixesForIri.remove(canonicalPrefix);
                if (canonicalPrefix.equals(activePrefix)) {
                    //iriToPrefixes.remove(prefixForIri);
                    iriToPrefixes.remove(iriForPrefix, canonicalPrefix);
                    if (iriToPrefixTrie != null) { iriToPrefixTrie.remove(iriForPrefix); }
                }
            }
            ++generation;
        }
    }

    @Override
    public String get(String prefix) {
        Objects.requireNonNull(prefix);
        String canonicalPrefix = PrefixLib.canonicalPrefix(prefix);
        String result;
        if (rwl == null) {
            // Case 1: No locking in use
            result = nonLockingGet(canonicalPrefix);
        } else if (delta == null && prefixToIri instanceof ConcurrentMap) {
            // Case 2: Skip locking if there is no delta and prefixToIri is already thread-safe
            result = prefixToIri.get(canonicalPrefix);
        } else {
            // Case 3: Apply locking
            result = calculate(readLock(), () -> nonLockingGet(canonicalPrefix));
        }
        return result;
    }

    private String nonLockingGet(String prefix) {
        String result = null;
        if (delta != null) {
            Optional<String> tmp = delta.get(prefix);
            result = tmp != null ? tmp.orElse(null) : prefixToIri.get(prefix);
        } else {
            result = prefixToIri.get(prefix);
        }
        return result;
    }

    /** Apply the delta */
    private void materialize() {
        delta.forEach((prefix, iriOpt) -> {
            if (iriOpt.isEmpty()) {
                materializingDelete(prefix);
            } else {
                materializingAdd(prefix, iriOpt.get());
            }
        });
        delta.clear();
        deltaSizeContrib = 0;
    }

    private static class WriteLockNeeded extends RuntimeException {
        /** Filling the stack trace which is a is very costly operation is skipped.
         * This exception should never be exposed to the outside; it is only
         * used for internal signaling. */
        @Override public Throwable fillInStackTrace() { return this ; }
    }

    @Override
    public Pair<String, String> abbrev(String iriStr) {
        Objects.requireNonNull(iriStr);
        Pair<String, String> result;
        if (rwl == null) {
            result = nonLockingAbbrev(iriStr, true);
        } else {
            // If locking is enabled then optimistically lock for reading first
            try {
                result = calculate(rwl.readLock(), () -> nonLockingAbbrev(iriStr, false));
            } catch (WriteLockNeeded e) {
                result = calculate(rwl.writeLock(), () -> nonLockingAbbrev(iriStr, true));
            }
        }
        return result;
    }

    private Pair<String, String> nonLockingAbbrev(String iriStr, boolean update) {
        // TODO If delta is non null every abbrev gets a write lock
        // Set up reverse lookup if it hasn't happened yet
        if (delta != null && !delta.isEmpty()) {
            if (update) {
                materialize();
            } else {
                throw new WriteLockNeeded();
            }
        }
        if (iriToPrefixTrie == null) {
            if (update) {
                initIriToPrefixLookup();
            } else {
                throw new WriteLockNeeded();
            }
        }

        Pair<String, String> r = null;

        String prefix = performPrefixLookup(iriStr);
        String iriForPrefix = prefix != null ? prefixToIri.get(prefix) : null;

        // Post process a found solution
        if (prefix != null && iriForPrefix != null) {
            String localName = iriStr.substring(iriForPrefix.length());
            if (PrefixLib.isSafeLocalPart(localName)) {
                r = Pair.create(prefix, localName);
            }
        }
        return r;
    }

    @Override
    public String abbreviate(String iriStr) {
        Objects.requireNonNull(iriStr);
        String result = null;
        Pair<String, String> prefixAndLocalName = abbrev(iriStr);
        if (prefixAndLocalName != null) {
            String prefix = prefixAndLocalName.getLeft();
            String ln = prefixAndLocalName.getRight();
            // Safe for RDF/XML as well
            if (strSafeFor(ln, ':')) {
                result = prefix + ":" + ln;
            }
        }
        return result;
    }

    /** Returns an unmodifiable and non-synchronized(!) view of the mappings */
    @Override
    public Map<String, String> getMapping() {
        if (delta != null) {
            execute(writeLock(), this::materialize);
        }
        return prefixToIriView;
    }

    @Override
    public Map<String, String> getMappingCopy() {
        Map<String, String> result = calculate(readLock(), () -> Map.copyOf(prefixToIri));
        return result;
    }

    @Override
    public void clear() {
        execute(writeLock(), () -> {
            if (!prefixToIri.isEmpty()) {
                prefixToIri.clear();
                if (iriToPrefixes != null) { iriToPrefixes.clear(); }
                if (iriToPrefixTrie != null) { iriToPrefixTrie.clear(); }
                cache.invalidateAll();
                ++generation;
            }
        });
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
        // return calculate(readLock(), () -> prefixToIri.isEmpty());
    }

    @Override
    public int size() {
        return prefixToIri.size() + deltaSizeContrib;
    }

    @Override
    public boolean containsPrefix(String prefix) {
        boolean result = rwl == null
                ? nonLockingContainsPrefix(prefix)
                : calculate(rwl.readLock(), () -> nonLockingContainsPrefix(prefix));
        return result;
    }

    private boolean nonLockingContainsPrefix(String prefix) {
        Objects.requireNonNull(prefix);
        boolean result;
        if (delta != null) {
            Optional<String> tmp = delta.get(prefix);
            result = tmp != null ? tmp.isPresent() : prefixToIri.containsKey(prefix);
        } else {
            result = prefixToIri.containsKey(prefix);
        }
        return result;
    }


    /**
     * Takes a guess for the namespace URI string to use in abbreviation.
     * Finds the part of the IRI string before the last '#' or '/'.
     *
     * @param iriString String string
     * @return String or null
     */
    protected String getPossibleKey(String iriString) {
        int n = iriString.length();
        int i;
        outer: for (i = n - 1; i >= 0; --i) {
            char c = iriString.charAt(i);
            for (int j = 0; j < fastTrackSeparators.length; ++j) {
                if (c == fastTrackSeparators[j]) {
                    break outer;
                }
            }
        }
        String result = i >= 0 ? iriString.substring(0, i + 1) : null;
        return result;
    }

    private String performPrefixLookup(String iriStr) {
        String prefix = null;

        if (enableFastTrack) {
            String possibleIriForPrefix = getPossibleKey(iriStr);
            // Try fast track first - if it produces a hit then
            // no overhead writing to the cache is needed
            // The drawback is that we do not necessarily get the longest prefix
            if (possibleIriForPrefix != null) {
                Collection<String> prefixes = iriToPrefixes.get(possibleIriForPrefix);
                prefix = getPrefix(prefixes);
            }
        }

        // If no solution yet then search for longest prefix
        if (prefix == null && enableTrie) {
            prefix = cache != null
                    ? cachedPrefixLookup(iriStr).orElse(null)
                    : uncachedPrefixLookup(iriStr);
        }
        return prefix;
    }

    private String getPrefix(Collection<String> prefixes) {
        String result = useLastPrefixForIri
                ? Iterables.getLast(prefixes, null)
                : Iterables.getFirst(prefixes, null);
        return result;
    }

    private Optional<String> cachedPrefixLookup(String iri) {
        if (cacheVersion != generation) {
            cache.invalidateAll();
            cacheVersion = generation;
        }

        Optional<String> prefix;
        try {
            prefix = cache.get(iri, () -> Optional.ofNullable(uncachedPrefixLookup(iri)));
        } catch (ExecutionException e) {
            throw new RuntimeException("Unexpected failure during cache lookup", e);
        }
        return prefix;
    }

    private String uncachedPrefixLookup(String iriStr) {
        String prefix = iriToPrefixTrie.longestMatch(iriStr);
        return prefix;
    }

    /** TS_Graph / TestPrefixMappingPrefixMap.testRetursSelf dead locks because it changes the map within forEach
     * The map contract discourages self modifications within compute (TODO does it also for forEach?)
     * TODO Runs the action within a read lock. Attempt to modify a locking map will result in a dead lock!
     *
     */
    @Override
    public void forEach(BiConsumer<String, String> action) {
        execute(writeLock(), () -> {
            if (delta == null || delta.isEmpty()) {
                prefixToIri.forEach(action);
            } else {
                // First, iterate all entries in delta that are not mapped to Optional.empty()
                delta.entrySet().stream().filter(e -> e.getValue().isPresent()).forEach(e -> action.accept(e.getKey(), e.getValue().get()));
                // Then, iterate the prefixToIri entries whose keys are not contained in delta
                prefixToIri.entrySet().stream().filter(e -> !delta.containsKey(e.getKey()));
            }
        });
    }

    @Override
    public void putAll(PrefixMap pmap) {
        if (pmap != this) {
            execute(writeLock(), () -> {
                pmap.forEach(this::nonLockingAdd);
            });
        }
    }

    @Override
    public void putAll(PrefixMapping pmap) {
        putAll(pmap.getNsPrefixMap());
    }

    @Override
    public void putAll(Map<String, String> mapping) {
        if (this.prefixToIri != mapping) {
            execute(writeLock(), () -> {
                mapping.forEach(this::nonLockingAdd);
            });
        }
    }

    private static void execute(Lock lock, Runnable runnable) {
        if (lock == null) {
            runnable.run();
        } else {
            lock.lock();
            try {
                runnable.run();
            } finally {
                lock.unlock();
            }
        }
    }

    private static <T> T calculate(Lock lock, Supplier<T> supplier) {
        T result;
        if (lock == null) {
            result = supplier.get();
        } else {
            lock.lock();
            try {
                result = supplier.get();
            } finally {
                lock.unlock();
            }
        }
        return result;
    }
}
