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

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.jena.shared.PrefixMapping;

/**
 * Factory which provides prefix maps
 *
 */
public class PrefixMapFactory {
    /**
     * Creates a new prefix map.
     * <p>
     * Will use whatever the version of ARQ you are using considers the default
     * implementation, this may change from release to release.
     * </p>
     *
     * @return Prefix Map
     */
    public static PrefixMap create() {
        return new PrefixMapStd();
    }

    /**
     * Creates a new prefix map which starts with a copy of an existing prefix
     * map.
     * <p>
     * Will use whatever the version of ARQ you are using considers the default
     * implementation, this may change from release to release.
     * </p>
     *
     * @param pmap
     *            Prefix Map to copy
     *
     * @return Prefix Map
     */
    public static PrefixMap create(PrefixMap pmap) {
        return new PrefixMapStd(pmap);
    }

    /**
     * Creates a new prefix map which starts
     * with a copy of an existing map.
     * <p>
     * Will use whatever the version of ARQ you are using considers the default
     * implementation, this may change from release to release.
     * </p>
     *
     * @param pmap
     *            PrefixMapping to copy
     *
     * @return Prefix Map
     */
    public static PrefixMap create(PrefixMapping pmap) {
        PrefixMap created = create();
        created.putAll(pmap);
        return created;
    }

    /**
     * Creates a new prefix map,initialized from a Map of prefix to IRI string.
     * <p>
     * Will use whatever the version of ARQ you are using considers the default
     * implementation, this may change from release to release.
     * </p>
     *
     * @param pmap Mapping from prefix to IRI string
     * @return Prefix Map
     */
    public static PrefixMap create(Map<String, String> pmap) {
        PrefixMap created = create();
        created.putAll(pmap);
        return created;
    }

    /**
     * Creates a new prefix map which is intended for use in output
     * <p>
     * Will use whatever the version of ARQ you are using considers the best
     * implementation for output, this may change from release to release.
     * </p>
     *
     * @return Prefix Map
     */
    public static PrefixMap createForOutput() {
        return new PrefixMapStd();
    }

    /**
     * Creates a new prefix map which is intended for use in output which starts
     * with a copy of an existing map
     * <p>
     * Will use whatever the version of ARQ you are using considers the best
     * implementation for output, this may change from release to release.
     * </p>
     *
     * @param pmap
     *            Prefix Map to copy
     *
     * @return Prefix Map
     */
    public static PrefixMap createForOutput(PrefixMap pmap) {
        return new PrefixMapStd(pmap);
    }

    /**
     * Creates a new prefix map which is intended for use in output which starts
     * with a copy of an existing map.
     * <p>
     * Will use whatever the version of ARQ you are using considers the best
     * implementation for output, this may change from release to release.
     * </p>
     *
     * @param prefixMapping
     *            Prefix Map to copy
     *
     * @return Prefix Map
     */
    public static PrefixMap createForOutput(PrefixMapping prefixMapping) {
        PrefixMap created = createForOutput();
        created.putAll(prefixMapping);
        return created;
    }

    /**
     * Creates a new prefix map, initialized from a Map of prefix to IRI string.
     *
     * @param pmap Mapping from prefix to IRI string
     * @return Prefix Map
     */
    public static PrefixMap createForOutput(Map<String, String> pmap) {
        PrefixMap created = createForOutput();
        created.putAll(pmap);
        return created;
    }

    /** Return an immutable view of the prefix map.
     * Throws {@link UnsupportedOperationException} on
     * attempts to update it.  Reflects changes made to the underlying map.
     * @param pmap  PrefixMap
     * @return Prefix Map
     */
    public static PrefixMap unmodifiablePrefixMap(PrefixMap pmap)
    {
        return new PrefixMapUnmodifiable(pmap) ;
    }

    /** Return an always-empty and immutable prefix map
     * @return Prefix Map
     */
    public static PrefixMap emptyPrefixMap()
    {
        return PrefixMapZero.empty ;
    }


    public static PrefixMapBuilder builder() {
        return new PrefixMapBuilder();
    }

    public static class PrefixMapBuilder {
        private long cacheSize;
        private boolean useDelta;
        private boolean isFastTrackEnabled;
        private boolean isTrieEnabled;
        private boolean eagerIriToPrefix;
        private boolean lastIriWins;
        private boolean initTrieLazily;
        private boolean useLocking;
        private char fastTrackSeparators[];
        private Supplier<Map<String, String>> mapFactory;

        PrefixMapBuilder() {
            this.cacheSize = PrefixMapStd.DFT_CACHE_SIZE;
            this.useDelta = PrefixMapStd.DFT_DELTA_ENABLED;
            this.isFastTrackEnabled = PrefixMapStd.DFT_FAST_TRACK_ENABLED;
            this.fastTrackSeparators = PrefixMapStd.DFT_FAST_TRACK_SEPARATORS;
            this.isTrieEnabled = PrefixMapStd.DFT_TRIE_ENABLED;
            this.eagerIriToPrefix = PrefixMapStd.DFT_EAGER_IRI_TO_PREFIX;
            this.lastIriWins = PrefixMapStd.DFT_LAST_IRI_WINS;
            this.initTrieLazily = PrefixMapStd.DFT_INIT_TRIE_LAZILY;
            this.useLocking = PrefixMapStd.DFT_LOCKING_ENABLED;
            this.mapFactory = PrefixMapStd.DFT_MAP_FACTORY;
        }

        public Supplier<Map<String, String>> getMapFactory() {
            return mapFactory;
        }

        public PrefixMapBuilder setMapFactory(Supplier<Map<String, String>> mapFactory) {
            this.mapFactory = mapFactory;
            return this;
        }

        public boolean isUseDelta() {
            return useDelta;
        }

        /** Update reverse-lookup data structures incrementally and only when needed.
         * Changes are tracked in a prefix-to-iri map. A reverse lookup materializes them.
         */
        public PrefixMapBuilder setUseDelta(boolean useDelta) {
            this.useDelta = useDelta;
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

        public boolean isFastTrackEnabled() {
            return isFastTrackEnabled;
        }

        public PrefixMapBuilder setFastTrackEnabled(boolean fastTrackEnabled) {
            this.isFastTrackEnabled = fastTrackEnabled;
            return this;
        }

        public char[] getFastTrackSeparators() {
            return fastTrackSeparators;
        }

        public PrefixMapBuilder setFastTrackSeparators(char[] fastTrackSeparators) {
            this.fastTrackSeparators = fastTrackSeparators;
            return this;
        }

        public boolean isTrieEnabled() {
            return isTrieEnabled;
        }

        public PrefixMapBuilder setTrieEnabled(boolean trieEnabled) {
            this.isTrieEnabled = trieEnabled;
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

        /** */
        public PrefixMapBuilder setLastPrefixWins(boolean lastPrefixWins) {
            this.lastIriWins = lastPrefixWins;
            return this;
        }


        public boolean isInitTrieLazily() {
            return initTrieLazily;
        }

        /** If true then the internal trie structure is only initialized on the first reverse lookup.
         * Otherwise the trie is initialized eagerly which increases the cost of modifications */
        public void setInitTrieLazily(boolean initTrieLazily) {
            this.initTrieLazily = initTrieLazily;
        }

        public boolean isUseLocking() {
            return useLocking;
        }

        /** If true then created instances will internally use a {@link ReentrantReadWriteLock}
         *  to synchronize modifications and lookups */
        public PrefixMapBuilder setUseLocking(boolean useLocking) {
            this.useLocking = useLocking;
            return this;
        }

        public PrefixMap build() {
            // Rules:
            // if trie is disabled then don't use cache
            // if neither fast-track nor fastpath is enabled then reverse lookups would always yield null - should we raise invalid configuration?!

            return new PrefixMapStd(mapFactory.get(), useDelta, cacheSize, isFastTrackEnabled, fastTrackSeparators, isTrieEnabled, eagerIriToPrefix, lastIriWins, initTrieLazily, useLocking);
        }
    }
}
