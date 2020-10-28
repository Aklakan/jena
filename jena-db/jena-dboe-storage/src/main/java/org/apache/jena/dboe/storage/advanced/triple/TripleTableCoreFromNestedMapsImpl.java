/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.apache.jena.dboe.storage.advanced.triple;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.MapSupplier;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.util.NodeUtils;

/**
 * TripleTableCore implementation based on three nested maps
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class TripleTableCoreFromNestedMapsImpl
    implements TripleTableCore
{
    protected Map<Node, Map<Node, Map<Node, Triple>>> store;
    protected MapSupplier mapSupplier;

    public TripleTableCoreFromNestedMapsImpl() {
        this(LinkedHashMap::new);
    }


//    @Override
//    public Meta2Node<TriplNode> getRootIndexNode(IndexNode<Node> parent) {
//        return IndexNodeForkFromMap.singleton(
//                parent, 0, forkS -> IndexNodeNestedMap.create(
//                    forkS, store, (nodeS, pMap) -> IndexNodeForkFromMap.singleton(
//                        nodeS, 1, forkP -> IndexNodeNestedMap.create(
//                            forkP, pMap, (nodeP, oMap) -> IndexNodeForkFromMap.singleton(
//                                nodeP, 2, forkO -> IndexNodeCollection.create(
//                                    forkO, oMap.keySet()))))));
//    }

    public TripleTableCoreFromNestedMapsImpl(MapSupplier mapSupplier) {
        super();
        this.mapSupplier = mapSupplier;

        // Initialize
        store = mapSupplier.get();
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void add(Triple triple) {
        add(store, triple, mapSupplier);
    }

    @Override
    public void delete(Triple triple) {
        delete(store, triple);
    }

    @Override
    public Stream<Triple> find(Node s, Node p, Node o) {
        return find(store, s, p, o);
    }


    /**
     * Stream all triples from nested maps
     *
     * @param store
     * @param triple
     */
    public static <T> Stream<T> find(
            Map<Node, Map<Node, Map<Node, T>>> store,
            Node s, Node p, Node o)
    {
        Stream<T> result = match(match(match(Stream.of(store),
                NodeUtils::isNullOrAny, s), NodeUtils::isNullOrAny, p), NodeUtils::isNullOrAny, o);

        return result;
    }

    /**
     * Add a triple to nested maps
     *
     * @param store
     * @param triple
     */
    public static void add(
        Map<Node, Map<Node, Map<Node, Triple>>> store,
        Triple triple,
        MapSupplier mapSupplier
    ) {
        store
            .computeIfAbsent(triple.getSubject(), s -> mapSupplier.get())
            .computeIfAbsent(triple.getPredicate(), p -> mapSupplier.get())
            .computeIfAbsent(triple.getObject(), o -> triple);
    }

    public static boolean contains(
            Map<Node, Map<Node, Map<Node, Map<Node, Triple>>>> store,
            Triple triple
        ) {
            boolean result = store
                .getOrDefault(triple.getSubject(), Collections.emptyMap())
                .getOrDefault(triple.getPredicate(), Collections.emptyMap())
                .containsKey(triple.getObject());

            return result;
        }

    /**
     * Delete a triple from nested maps
     *
     * @param store
     * @param triple
     */
    public static void delete(
        Map<Node, Map<Node, Map<Node, Triple>>> store,
        Triple triple)
    {
        Map<Node, Map<Node, Triple>> pm = store.getOrDefault(triple.getSubject(), Collections.emptyMap());
        Map<Node, Triple> om = pm.getOrDefault(triple.getPredicate(), Collections.emptyMap());

        if(om.containsKey(triple.getObject())) {
            om.remove(triple.getObject());
            if(om.isEmpty()) { pm.remove(triple.getPredicate()); }
            if(pm.isEmpty()) { store.remove(triple.getSubject()); }
        }
    }

    public static <K, V> Stream<V> match(Stream<Map<K, V>> in, Predicate<? super K> isAny, K k) {
        boolean any = isAny.test(k);
        Stream<V> result = any ? in.flatMap(m -> m.values().stream())
                : in.flatMap(m -> m.containsKey(k) ? Stream.of(m.get(k)) : Stream.empty());

        return result;
    }

}
