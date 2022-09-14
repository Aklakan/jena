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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.lib.Trie;
import org.apache.jena.ext.com.google.common.base.Preconditions;
import org.apache.jena.ext.com.google.common.cache.Cache;
import org.apache.jena.ext.com.google.common.cache.CacheBuilder;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.graph.PrefixMappingBase;

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
public class PrefixMapStd extends PrefixMapBase {

    // See the setters of PrefixMapBuilder for explanations of these constants

    public static final int DFT_CACHE_SIZE = 1000;
    public static final boolean DFT_ENABLE_FAST_TRACK = true;
    public static final boolean DFT_EAGER_IRI_TO_PREFIX = true;
    public static final boolean DFT_LAST_IRI_WINS = true;
    public static final boolean DFT_INIT_TRIE_LAZILY = true;
    public static final boolean DFT_USE_LOCKING = true;
    public static final Supplier<Map<String, String>> DFT_MAP_FACTORY = ConcurrentHashMap::new;

    /** Prefix-to-iri mapping. never null. */
    private final Map<String, String> prefixToIri;
    private final Map<String, String> prefixToIriView;

    /** The map used for fast track lookups.
     * For exact matches of IRI namespaces the map is much faster than the trie. */
    private Map<String, String> iriToPrefixMap = null;

    /** Trie for longest prefix lookups; may be initialized lazily when needed */
    private Trie<String> iriToPrefixTrie = null;

    /** Cache for mapping iris to prefixes.
     * Wrapping with Optional is needed because the Guava Cache does not allow for null values */
    private final Cache<String, Optional<String>> cache;

    /** Disabling fast track always resorts to longest prefix lookup */
    private final boolean enableFastTrack;

    /** If override is enabled then after "add(p1, iri); add(p2, iri);" iri will map to p2; p1 otherwise. */
    private final boolean lastPrefixWins;

    /** Locking is optional */
    private final ReadWriteLock rwl;

    /** A generation counter that is incremented on modifications and which is
     * used to invalidate the internal cache when needed.
     * If generation and cacheVersion differ then the next prefix lookup will invalidate the cache and
    /* set cacheVersion to generation */
    private int generation = 0;
    private int cacheVersion = 0;

    public PrefixMapStd() {
        this(DFT_CACHE_SIZE);
    }

    /** Sets up a default PrefixMapStd and copies the prefixes from the argument. */
    public PrefixMapStd(PrefixMap prefixMap) {
        this(DFT_CACHE_SIZE);
        putAll(prefixMap);
    }

    public PrefixMapStd(long cacheSize) {
        this(DFT_MAP_FACTORY.get(), cacheSize, DFT_ENABLE_FAST_TRACK, DFT_EAGER_IRI_TO_PREFIX, DFT_LAST_IRI_WINS, DFT_INIT_TRIE_LAZILY, DFT_USE_LOCKING);
    }

    private void initIriToPrefixMap() {
        iriToPrefixMap = new HashMap<>();
        prefixToIri.forEach((prefix, iri) -> iriToPrefixMap.compute(iri, (i, oldPrefix) -> lastPrefixWins || oldPrefix == null ? prefix : oldPrefix));
    }

    /** Initializes the trie data structure for faster (non-fast-track) iri-to-prefix lookup. */
    private void initIriToPrefixLookup() {
        if (iriToPrefixMap == null) {
            initIriToPrefixMap();
        }

        iriToPrefixTrie = new Trie<>();
        iriToPrefixMap.forEach(iriToPrefixTrie::add);
    }

    /**
     * Create a PrefixMapStd instance using the the specified prefix-to-iri map implementation and cache size.
     * @param prefixToIri An empty map into which to store prefixes. Should not be changed externally.
     * @param cacheSize The cache size for prefix lookups.
     */
    public PrefixMapStd(Map<String, String> prefixToIri, long cacheSize, boolean enableFastTrack, boolean eagerIriToPrefix , boolean addOverridesIriToPrefix, boolean initTrieLazily, boolean useLocking) {
        super();
        Objects.requireNonNull(prefixToIri);
        Preconditions.checkArgument(cacheSize >= 0, "Cache size must be non-negative");
        this.prefixToIri = prefixToIri;
        if (!this.prefixToIri.isEmpty()) {
            // Best effort check; the caller may still perform concurrent modifications to the supplied map
            throw new IllegalArgumentException("PrefixToIri map must be initially empty");
        }
        this.prefixToIriView = Collections.unmodifiableMap(prefixToIri);
        this.cache = cacheSize == 0
                ? null
                : CacheBuilder.newBuilder().maximumSize(cacheSize).build();
        this.enableFastTrack = enableFastTrack;
        this.lastPrefixWins = addOverridesIriToPrefix;
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

    /** If the iri is already mapped to a different prefix then depending on the setup either the first or last one wins */
    @Override
    public void add(String prefix, String iri) {
        execute(writeLock(), () -> {
            addInternal(prefix, iri);
        });
    }

    /** Internal, non-locking "add" method. This method is called repeatedly during bulk inserts. */
    private void addInternal(String prefix, String iri) {
        Objects.requireNonNull(prefix);
        Objects.requireNonNull(iri);
        String canonicalPrefix = PrefixLib.canonicalPrefix(prefix);
        String oldIri = prefixToIri.get(canonicalPrefix);
        if (!Objects.equals(oldIri, iri)) {
            prefixToIri.put(canonicalPrefix, iri);
            // If there was a non-null oldIri and we are in override mode then remove oldIri
            if (oldIri != null && lastPrefixWins) {
                if (iriToPrefixMap != null) { iriToPrefixMap.remove(oldIri); }
                if (iriToPrefixTrie != null) { iriToPrefixTrie.remove(oldIri); }
            }

            // If in override mode or if the iri was not mapped to a prefix then update
            if (lastPrefixWins || (iriToPrefixMap != null && iriToPrefixMap.get(iri) == null)) {
                if (iriToPrefixMap != null) { iriToPrefixMap.put(iri, canonicalPrefix); }
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
        Objects.requireNonNull(prefix);
        String canonicalPrefix = PrefixLib.canonicalPrefix(prefix);
        execute(writeLock(), () -> {
            // Removal returns the previous value or null if there was none
            String iriForPrefix = prefixToIri.remove(canonicalPrefix);
            if (iriForPrefix != null) {
                if (iriToPrefixMap != null) {
                    String prefixForIri = iriToPrefixMap.get(iriForPrefix);
                    if (canonicalPrefix.equals(prefixForIri)) {
                        iriToPrefixMap.remove(prefixForIri);
                        if (iriToPrefixTrie != null) { iriToPrefixTrie.remove(iriForPrefix); }
                    }
                }
                ++generation;
            }
        });
    }

    @Override
    public Pair<String, String> abbrev(String iriStr) {
        Objects.requireNonNull(iriStr);

        // Set up iri-to-prefix-lookup if it hasn't happened yet
        if (iriToPrefixTrie == null) {
            execute(writeLock(), this::initIriToPrefixLookup);
        }

        return calculate(readLock(), () -> {
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
        });
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

    @Override
    public String get(String prefix) {
        Objects.requireNonNull(prefix);
        String canonicalPrefix = PrefixLib.canonicalPrefix(prefix);
        String result = prefixToIri instanceof ConcurrentMap
                ? prefixToIri.get(canonicalPrefix)
                : calculate(readLock(), () -> prefixToIri.get(canonicalPrefix));
        return result;
    }

    /** Returns an unmodifiable and non-synchronized(!) view of the mappings */
    @Override
    public Map<String, String> getMapping() {
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
                if (iriToPrefixMap != null) { iriToPrefixMap.clear(); }
                if (iriToPrefixTrie != null) { iriToPrefixTrie.clear(); }
                cache.invalidateAll();
                ++generation;
            }
        });
    }

    @Override
    public boolean isEmpty() {
        return calculate(readLock(), () -> prefixToIri.isEmpty());
    }

    @Override
    public int size() {
        return calculate(readLock(), () -> prefixToIri.size());
    }

    @Override
    public boolean containsPrefix(String prefix) {
        Objects.requireNonNull(prefix);
        return calculate(readLock(), () -> {
            String canonicalPrefix = PrefixLib.canonicalPrefix(prefix);
            return prefixToIri.containsKey(canonicalPrefix);
        });
    }

    /**
     * Takes a guess for the namespace URI string to use in abbreviation.
     * Finds the part of the IRI string before the last '#' or '/'.
     *
     * @param iriString String string
     * @return String or null
     */
    protected static String getPossibleKey(String iriString) {
        int n = iriString.length();
        int i;
        for (i = n - 1; i >= 0; --i) {
            char c = iriString.charAt(i);
            if (c == '#' || c == '/') {
                // We could add ':' here, it is used as a separator in URNs.
                // But it is a multiple use character and always present in the scheme name.
                // This is a fast-track guess so don't try guessing based on ':'.
                break;
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
                prefix = iriToPrefixMap.get(possibleIriForPrefix);
            }
        }

        // If no solution yet then search for longest prefix
        if (prefix == null) {
            prefix = cache != null
                    ? cachedPrefixLookup(iriStr).orElse(null)
                    : uncachedPrefixLookup(iriStr);
        }
        return prefix;
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

    /** Runs the action within a read lock. */
    @Override
    public void forEach(BiConsumer<String, String> action) {
        execute(readLock(), () -> {
            prefixToIri.forEach(action);
        });
    }

    @Override
    public void putAll(PrefixMap pmap) {
        if (pmap != this) {
            execute(writeLock(), () -> {
                pmap.forEach(this::addInternal);
            });
        }
    }

    @Override
    public void putAll(PrefixMapping pmap) {
        putAll(pmap.getNsPrefixMap());
    }

    @Override
    public void putAll(Map<String, String> mapping) {
        execute(writeLock(), () -> {
            mapping.forEach(this::addInternal);
        });
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

    public static PrefixMapBuilder builder() {
        return new PrefixMapBuilder();
    }

    public static class PrefixMapBuilder {
        private long cacheSize;
        private boolean enableFastTrack;
        private boolean eagerIriToPrefix;
        private boolean lastIriWins;
        private boolean initTrieLazily;
        private boolean useLocking;
        private Supplier<Map<String, String>> mapFactory;

        PrefixMapBuilder() {
            this.cacheSize = DFT_CACHE_SIZE;
            this.enableFastTrack = DFT_ENABLE_FAST_TRACK;
            this.eagerIriToPrefix = DFT_EAGER_IRI_TO_PREFIX;
            this.lastIriWins = DFT_LAST_IRI_WINS;
            this.initTrieLazily = DFT_INIT_TRIE_LAZILY;
            this.useLocking = DFT_USE_LOCKING;
            this.mapFactory = DFT_MAP_FACTORY;
        }

        public Supplier<Map<String, String>> getMapFactory() {
            return mapFactory;
        }

        public PrefixMapBuilder setMapFactory(Supplier<Map<String, String>> mapFactory) {
            this.mapFactory = mapFactory;
            return this;
        }

        public long getCacheSize() {
            return cacheSize;
        }

        /** Set the cache size for longest prefix lookups during abbreviation. A value of 0 disables it.*/
        public PrefixMapBuilder setCacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public boolean isEnableFastTrack() {
            return enableFastTrack;
        }

        public PrefixMapBuilder setEnableFastTrack(boolean enableFastTrack) {
            this.enableFastTrack = enableFastTrack;
            return this;
        }

        public boolean isEagerIriToPrefix() {
            return eagerIriToPrefix;
        }

        /** Whether to initialize the reverse mapping from iri to prefix eagerly.
         * If lastIriWins semantics are desired but the prefixToIri map is not insert order preserving then
         * this flag must be true.
         * Conversely, if prefixToIri is insert order preserving then the correct reverse
         * mapping w.r.t. lastIriWins can be computed on-demand; thus saving the extra efforts if not needed.
         * If false then initialization happens on the first call to {@link PrefixMap#abbrev(String)} which
         * iterates the entries of prefixToIri in it's inherent order.
         * */
        public PrefixMapBuilder setEagerIriToPrefix(boolean eagerIriToPrefix) {
            this.eagerIriToPrefix = eagerIriToPrefix;
            return this;
        }

        public boolean isLastPrefixWins() {
            return lastIriWins;
        }

        public PrefixMapBuilder setLastPrefixWins(boolean lastPrefixWins) {
            this.lastIriWins = lastPrefixWins;
            return this;
        }

        public boolean isInitTrieLazily() {
            return initTrieLazily;
        }

        public void setInitTrieLazily(boolean initTrieLazily) {
            this.initTrieLazily = initTrieLazily;
        }

        public boolean isUseLocking() {
            return useLocking;
        }

        public PrefixMapBuilder setUseLocking(boolean useLocking) {
            this.useLocking = useLocking;
            return this;
        }

        public PrefixMap build() {
            return new PrefixMapStd(mapFactory.get(), cacheSize, enableFastTrack, eagerIriToPrefix, lastIriWins, initTrieLazily, useLocking);
        }
    }
}
