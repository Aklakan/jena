/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.sparql.exec.adapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.QueryExecBuilder;
import org.apache.jena.sparql.exec.QueryExecDatasetBuilderImpl;
import org.apache.jena.sparql.exec.UpdateExecBuilder;
import org.apache.jena.sparql.exec.UpdateExecDatasetBuilderImpl;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry of {@link QueryExecBuilderProvider} and {@link UpdateExecBuilderProvider}
 * implementations that can create SPARQL execution builders.
 *
 * <p>When a client asks the registry for a builder it proceeds as follows:
 *
 * <ol>
 *   <li>Iterate over the registered providers <i>in reverse registration order</i>
 *       (the most‑recently added provider is consulted first).</li>
 *   <li>The first provider whose {@link QueryExecBuilderProvider#accept(DatasetGraph,Context)}
 *       (or {@link UpdateExecBuilderProvider#accept(DatasetGraph,Context)}) method returns {@code true}
 *       is selected.</li>
 *   <li>If no registered provider accepts the supplied {@link DatasetGraph} and {@link Context},
 *       the registry throws {@link java.util.NoSuchElementException}.</li>
 *   <li>The global registry returned by {@link #get()} is pre‑populated with Jena’s built‑in
 *       ARQ providers (these use the machinery of {@code org.apache.jena.engine}).
 *       These default providers operate directly on a {@link DatasetGraph} and serve
 *       as a catch‑all when user‑registered providers do not accept the request.</li>
 * </ol>
 *
 * <p>Supported provider types are:
 *
 * <ul>
 *   <li>{@link QueryExecBuilderProvider}
 *       – creates {@link QueryExecBuilder} instances.</li>
 *   <li>{@link UpdateExecBuilderProvider}
 *       – creates {@link UpdateExecBuilder} instances.</li>
 * </ul>
 *
 * <p>Implementations may be added at runtime via {@link #add(QueryExecBuilderProvider)}
 * or {@link #add(UpdateExecBuilderProvider)}.
 *
 * @see QueryExecBuilderProvider
 * @see UpdateExecBuilderProvider
 * @since 6.1.0
 */public class SparqlAdapter {
    private static final Logger logger = LoggerFactory.getLogger(SparqlAdapter.class);

    List<QueryExecBuilderProvider> queryProviders = Collections.synchronizedList(new ArrayList<>());
    List<UpdateExecBuilderProvider> updateProviders = Collections.synchronizedList(new ArrayList<>());

    // Singleton
    private static SparqlAdapter registry;
    static { init(); }

    static public SparqlAdapter get() {
        return registry;
    }

    public List<QueryExecBuilderProvider> getQueryProviders() {
        return queryProviders;
    }

    public List<UpdateExecBuilderProvider> getUpdateProviders() {
        return updateProviders;
    }

    /** If there is a sparql adapter registry in the context then return it otherwise yield the global instance */
    static public SparqlAdapter chooseRegistry(Context context) {
        SparqlAdapter result = get(context);
        if (result == null) {
            result = get();
        }
        return result;
    }

    /** Get the sparql adapter registry from the context or null if there is none.
     *  Returns null if the context is null. */
    static public SparqlAdapter get(Context context) {
        SparqlAdapter result = context == null
                ? null
                : context.get(ARQConstants.registrySparqlAdapters);
        return result;
    }

    static public void set(Context context, SparqlAdapter registry) {
        context.set(ARQConstants.registrySparqlAdapters, registry);
    }

    public SparqlAdapter copy() {
        SparqlAdapter result = new SparqlAdapter();
        result.queryProviders.addAll(queryProviders);
        result.updateProviders.addAll(updateProviders);
        return result;
    }

    /** Create a copy of the registry from the context or return a new instance */
    public static SparqlAdapter copyFrom(Context context) {
        SparqlAdapter tmp = get(context);
        SparqlAdapter result = tmp != null
                ? tmp.copy()
                : new SparqlAdapter();
        return result;
    }

    public SparqlAdapter() { }

    private static void init() {
        registry = new SparqlAdapter();

        registry.add(getDefaultQueryProvider());
        registry.add(getDefaultUpdateProvider());
    }

    // ----- Query -----

    public static QueryExecBuilderProvider getDefaultQueryProvider() {
        return QueryExecBuilderProviderMain.get();
    }

    /** Add a query execution builder provider to the default registry. */
    public static void addProvider(QueryExecBuilderProvider f) { get().add(f); }

    /** Add a query execution builder provider. */
    public void add(QueryExecBuilderProvider f) {
        // Add to low end so that newer factories are tried first
        queryProviders.add(0, f);
    }

    /** Remove a query execution builder provider from the default registry. */
    public static void removeProvider(QueryExecBuilderProvider f)  { get().remove(f); }

    /** Remove a query execution builder provider. */
    public void remove(QueryExecBuilderProvider f)  { queryProviders.remove(f); }

    /** Check whether a query execution builder provider is registered in the default registry. */
    public static boolean containsProvider(QueryExecBuilderProvider f) { return get().contains(f); }

    /** Check whether a query execution builder provider is already registered. */
    public boolean contains(QueryExecBuilderProvider f) { return queryProviders.contains(f); }

    public static QueryExecBuilder newQueryExecBuilder(DatasetGraph dsg, Context context) {
        /** If the dataset is null then use Jena's ARQ query engine. */
        if (dsg == null) {
            return QueryExecDatasetBuilderImpl.create().dataset(dsg);
        }

        // XXX Unwrap Graph view over a DatasetGraph?

        Context cxt = dsg.getContext();
        SparqlAdapter registry = chooseRegistry(cxt);

        QueryExecBuilder result = null;
        for (QueryExecBuilderProvider provider : registry.queryProviders) {
            if (provider.accept(dsg, context)) {
                result = provider.create(dsg, context);
                if (result != null) {
                    break;
                } else {
                    logger.warn("Provider returned null: " + provider);
                }
            }
        }
        if (result == null) {
            throw new NoSuchElementException("No provider found for " + dsg.getClass());
        }
        return result;
    }

    private static class QueryExecBuilderProviderMain implements QueryExecBuilderProvider {
        private static final QueryExecBuilderProvider INSTANCE = new QueryExecBuilderProviderMain();
        public static QueryExecBuilderProvider get() { return INSTANCE; }

        private QueryExecBuilderProviderMain() {}

        @Override public boolean accept(DatasetGraph dsg, Context context) { return true; }

        @Override
        public QueryExecBuilder create(DatasetGraph dsg, Context context) {
            return QueryExecDatasetBuilderImpl.create().dataset(dsg).context(context);
        }
    }

    // ----- Update -----

    public static UpdateExecBuilderProvider getDefaultUpdateProvider() {
        return UpdateExecBuilderProviderMain.get();
    }

    /** Add an update execution builder provider to the default registry. */
    public static void addProvider(UpdateExecBuilderProvider f) { get().add(f); }

    /** Add an update execution builder provider. */
    public void add(UpdateExecBuilderProvider f) {
        // Add to low end so that newer factories are tried first
        updateProviders.add(0, f);
    }

    /** Remove an update execution builder provider from the default registry. */
    public static void removeProvider(UpdateExecBuilderProvider f)  { get().remove(f); }

    /** Remove an update execution builder provider. */
    public void remove(UpdateExecBuilderProvider f)  { updateProviders.remove(f); }

    /** Check whether an update execution builder provider is already registered in the default registry */
    public static boolean containsProvider(UpdateExecBuilderProvider f) { return get().contains(f); }

    /** Check whether an update execution builder provider is already registered. */
    public boolean contains(UpdateExecBuilderProvider f) { return updateProviders.contains(f); }

    public static UpdateExecBuilder newUpdateExecBuilder(DatasetGraph dsg, Context context) {
        /** If the dataset is null then use Jena's ARQ update engine. */
        if (dsg == null) {
            return UpdateExecDatasetBuilderImpl.create().dataset(dsg);
        }

        // XXX Unwrap Graph view over a DatasetGraph?

        Context cxt = dsg.getContext();
        SparqlAdapter registry = chooseRegistry(cxt);

        UpdateExecBuilder result = null;
        for (UpdateExecBuilderProvider provider : registry.updateProviders) {
            if (provider.accept(dsg, context)) {
                result = provider.create(dsg, context);
                if (result != null) {
                    break;
                } else {
                    logger.warn("Provider returned null: " + provider);
                }
            }
        }
        if (result == null) {
            throw new NoSuchElementException("No provider found for " + dsg.getClass());
        }
        return result;
    }

    private static class UpdateExecBuilderProviderMain implements UpdateExecBuilderProvider {
        private static final UpdateExecBuilderProvider INSTANCE = new UpdateExecBuilderProviderMain();
        public static UpdateExecBuilderProvider get() { return INSTANCE; }

        private UpdateExecBuilderProviderMain() {}

        @Override public boolean accept(DatasetGraph dsg, Context context) { return true; }

        @Override
        public UpdateExecBuilder create(DatasetGraph dsg, Context context) {
            return UpdateExecDatasetBuilderImpl.create().dataset(dsg).context(context);
        }
    }
}
