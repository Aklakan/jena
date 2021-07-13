package org.apache.jena.dboe.storage.advanced.core;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt2;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt3;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafComponentSet;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.postProcessAdd;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.Alt2;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.Alt3;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.MapSupplier;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.SetSupplier;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;

public class TripleStorages {

    public static StorageNodeMutable<Triple, Node, ?> createConventionalStorage() {
        return createConventionalStorage(HashMap::new);
    }

    /**
     * Create a conventional storage with three indexes - SPO, OPS and POS.
     */
    public static StorageNodeMutable<Triple, Node, ?> createConventionalStorage(MapSupplier mapSupplier) {
        StorageNodeMutable<Triple, Node, ?> storage =
                alt3(
                    // spo
                    innerMap(0, mapSupplier,
                        innerMap(1, mapSupplier,
                            leafMap(2, mapSupplier, TupleAccessorTriple.INSTANCE)))
                    ,
                    // ops
                    innerMap(2, mapSupplier,
                        innerMap(1, mapSupplier,
                            leafMap(0, mapSupplier, TupleAccessorTriple.INSTANCE)))
                    ,
                    // pos
                    innerMap(1, mapSupplier,
                        innerMap(2, mapSupplier,
                            leafMap(0, mapSupplier, TupleAccessorTriple.INSTANCE)))
                );

        return storage;
    }

    /**
     * A storage that indexes triples in a nested structure in all possible ways:
     * (S(P(O)|O(P)) | P(S(O)|O(S)) | O(P(S)|S(P)))
     *
     * @return
     */
    public static StorageNodeMutable<int[], Integer, ?> createHyperTrieStorageInt(
            TupleAccessor<int[], Integer> accessor) {

        // TODO The leaf set suppliers should be invoked with context information such that
        // different collation orders of the component indices can yield the same set
        // E.g. 1=rdf:type 2=foaf:Person can reuse the set for 2=foaf:Person 1=rdf:type

        StorageNodeMutable<int[], Integer,
            Alt3<
                Map<Integer, Alt2<Map<Integer, Set<Integer>>, Map<Integer, Set<Integer>>>>,
                Map<Integer, Alt2<Map<Integer, Set<Integer>>, Map<Integer, Set<Integer>>>>,
                Map<Integer, Alt2<Map<Integer, Set<Integer>>, Map<Integer, Set<Integer>>>>>
            >
        result = alt3(
            innerMap(0, Int2ObjectOpenHashMap::new, alt2(
                innerMap(1, Int2ObjectOpenHashMap::new, leafComponentSet(2, SetSupplier.forceCast(IntArraySet::new), accessor)),
                innerMap(2, Int2ObjectOpenHashMap::new, leafComponentSet(1, SetSupplier.forceCast(IntArraySet::new), accessor)))),
            innerMap(1, HashMap::new, alt2(
                innerMap(0, Int2ObjectOpenHashMap::new, leafComponentSet(2, SetSupplier.forceCast(IntArraySet::new), accessor)),
                innerMap(2, Int2ObjectOpenHashMap::new, leafComponentSet(0, SetSupplier.forceCast(IntArraySet::new), accessor)))),
            innerMap(2, HashMap::new, alt2(
                innerMap(0, Int2ObjectOpenHashMap::new, leafComponentSet(1, SetSupplier.forceCast(IntArraySet::new), accessor)),
                innerMap(1, Int2ObjectOpenHashMap::new, leafComponentSet(0, SetSupplier.forceCast(IntArraySet::new), accessor))))
        );

        return result;
    }


    public static <D, C> StorageNodeMutable<D, C, ?> createHyperTrieStorage(
            TupleAccessor<D, C> accessor) {
        return createHyperTrieStorage(accessor, HashMap::new, HashSet::new);
    }

    public static <D, C> StorageNodeMutable<D, C, ?> createHyperTrieStorageIdentity(
            TupleAccessor<D, C> accessor) {
        return createHyperTrieStorage(accessor, IdentityHashMap::new, Sets::newConcurrentHashSet);
    }

    /**
     * A storage that indexes triples in a nested structure in all possible ways:
     * (S(P(O)|O(P)) | P(S(O)|O(S)) | O(P(S)|S(P)))
     *
     * @return
     */
    public static <D, C> StorageNodeMutable<D, C, ?> createHyperTrieStorage(
            TupleAccessor<D, C> accessor, MapSupplier mapSupplier, SetSupplier setSupplier) {
        //TupleAccessor<Triple, Node> accessor = TupleAccessorTripleAnyToNull.INSTANCE;

        // TODO The leaf set suppliers should be invoked with context information such that
        // different collation orders of the component indices can yield the same set
        // E.g. 1=rdf:type 2=foaf:Person can reuse the set for 2=foaf:Person 1=rdf:type

        // go through the permutations of tuple components in order and check
        // whether for the last component there was already a prior entry with permutated parents
        // Example: For ps(o) we check whether we already encounteered a sp(o)
        // spo
        // sop
        // pso -> spo
        // pos
        // osp -> sop
        // ops -> pos

        SetSupplier spo = setSupplier;
        SetSupplier sop = setSupplier;
        SetSupplier pos = setSupplier;

        SetSupplier pso = SetSupplier.none(); // reuses spo
        SetSupplier osp = SetSupplier.none(); // reuses sop
        SetSupplier ops = SetSupplier.none(); // reuses pos

        StorageNodeMutable<D, C,
            Alt3<
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>,
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>,
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>>
            >
        result = postProcessAdd(alt3(
            innerMap(0, mapSupplier, alt2(
                innerMap(1, mapSupplier, leafComponentSet(2, spo, accessor)),
                innerMap(2, mapSupplier, leafComponentSet(1, sop, accessor)))),
            innerMap(1, mapSupplier, alt2(
                innerMap(0, mapSupplier, leafComponentSet(2, pso, accessor)),
                innerMap(2, mapSupplier, leafComponentSet(0, pos, accessor)))),
            innerMap(2, mapSupplier, alt2(
                innerMap(0, mapSupplier, leafComponentSet(1, osp, accessor)),
                innerMap(1, mapSupplier, leafComponentSet(0, ops, accessor))))
        ), (store, tup) -> {
            C s = accessor.get(tup, 0);
            C p = accessor.get(tup, 1);
            C o = accessor.get(tup, 2);

            Alt2<Map<C, Set<C>>, Map<C, Set<C>>> sm = store.getV1().get(s);
            Set<C> spom = sm.getV1().get(p);
            Set<C> sopm = sm.getV2().get(o);

            Alt2<Map<C, Set<C>>, Map<C, Set<C>>> pm = store.getV2().get(p);
            /* Set<C> psom = */ pm.getV1().put(s, spom);
            Set<C> posm = pm.getV2().get(o);

            Alt2<Map<C, Set<C>>, Map<C, Set<C>>> om = store.getV3().get(o);
            /* Set<C> ospm = */ om.getV1().put(s, sopm);
            /* Set<C> opsm = */ om.getV2().put(p, posm);

        });

        return result;
    }
}
