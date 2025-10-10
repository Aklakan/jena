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

package org.apache.jena.rdfs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.SetUtils;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorConcat;
import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.atlas.lib.tuple.Tuple2;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.rdfs.engine.CxtInf;
import org.apache.jena.rdfs.engine.MapperX;
import org.apache.jena.rdfs.engine.Match;
import org.apache.jena.rdfs.engine.MatchRDFS;
import org.apache.jena.rdfs.engine.MatchWrapper;
import org.apache.jena.rdfs.engine.TupleMapper3;
import org.apache.jena.rdfs.setup.ConfigRDFS;

// FIXME WIP

/**
 * RDFS stream reasoner engine that builds upon Jena's {@link MatchRDFS} but handles
 * the X_ANY_ANY and ANY_ANY_ANY cases differently in order to produce fewer duplicates.
 */
public class MatchRDFSReduced<X, T>
    extends MatchWrapper<X, T, Match<X, T>>
{
    /** The non-inferencing backend. Should be the base's base.
     * Note that the delegate of this wrapper backs the backend with RDFS inferences.
     * */
    private Match<X, T> backend;
    private MapperX<X, T> mapper; // Local reference obtained from backend.
    private TupleMapper3<X, T> tupleMapper;

    private CxtInf<X, T> cxtInf;
    private ConfigRDFS<X> setup;

    /**
     * Affects handling of X_ANY_ANY when encountering a tuple (s, p, o).
     * Whether to emit types for o based on the range types of p immediately.
     *
     * If the option is false:
     *   if o appears as a subject i.e. contains(o, _, _) == true then:
     *     Emit the o's types when encountering it as a subject.
     *   else
     *     Emit the o's range types immediately.
     *
     * Implications of the option value:
     *
     * true:
     *   + No contains checks.
     *   - Likely more duplicates when multiple.
     *     occurrences of the same o are farther apart than the cache size.
     *   - Breaks grouping by subject s; stream will be interleaved with (o rdf:type ?) triples.
     *
     * false:
     *   - A contains check for each o (check result will be cached).
     *   + Fewer duplicates.
     *   + Better grouping by subject.
     *     XXX Grouping by subject only broken if o does not appear as a subject.
     *         Such inferences could be deferred until encountered the next different subject or end of stream.
     *
     * Recommended: 'false'
     */
    protected boolean emitObjectTypesEagerly = false;

    /**
     * This attribute affects retrieval of a resource's unique set of properties
     * that is relevant for domain and range type inferences.
     *
     * Retrieval starts with a scan of the resource's triples.
     * Once more than (enumerateThresholdFactor * #properties_with_ranges_in_ontology) triples have
     * been iterated, the strategy is changed to enumeration of the remaining ontology's properties
     * and testing for which of them are linked to the resource.
     */
    protected float enumerationThresholdFactor = 30;

    protected MatchRDFSReduced(Match<X, T> matchRDFS, Match<X, T> backend, CxtInf<X, T> cxtInf) {
        super(matchRDFS);
        this.cxtInf = cxtInf;
        this.backend = backend;

        // Copy to local fields for nicer access.
        this.setup = cxtInf.setup;
        this.mapper = backend.getMapper();
        this.tupleMapper = backend.getTupleMapper();
    }

    public static <C, D> Match<C, D> create(ConfigRDFS<C> setup, MapperX<C, D> mapper, Match<C, D> backend) {
        // InfFindTuple is also a CxtInf
        InfFindTuple<C, D> matchRDFS = new InfFindTuple<>(setup, mapper, backend);
        return new MatchRDFSReduced<>(matchRDFS, backend, matchRDFS);
    }

    @Override
    public Stream<T> match(X s, X p, X o) {
        return (!cxtInf.setup.hasRDFS())
            ? backend.match(s, p, o)
            : matchWithInf(s, p, o);
    }

    public Stream<T> matchWithInf(X s, X p, X o) {
        Stream<T> result;
        boolean isRdfType = cxtInf.rdfType.equals(p);

        // If we ask for RDF type and there are domain/range declarations then
        // we need to look at all properties.
        Worker_S_ANY_ANY worker = new Worker_S_ANY_ANY(s);
        if (isTerm(p)) {
            if (isRdfType) {
                if (isTerm(o)) {
                    if (isTerm(s)) {
                        result = worker.find();
                        result = result.filter(t -> cxtInf.rdfType.equals(mapper.predicate(t)) && o.equals(mapper.object(t)));
                    } else {
                        Set<X> typePs = expandOrSelf(cxtInf.rdfType, setup::getSubPropertiesInc);
                        result = Iter.asStream(Iter.map(uniqueSubjectsOf_P_O(typePs, o), ss -> dstCreate(ss, cxtInf.rdfType, o)));
                    }
                } else {
                    result = worker.find();
                    result = result.filter(t -> cxtInf.rdfType.equals(mapper.predicate(t)));
                }
            } else {
                result = Iter.asStream(find_X_notType_X(s, p, o));
            }
        } else { // p == ANY
            if (isTerm(o)) {
                if (isTerm(s)) {
                    result = worker.find();
                    result = result.filter(t -> mapper.object(t).equals(o));
                } else {
                    result = Iter.asStream(worker.find_ANY_ANY_O(o));
                }
            } else { // X ANY ANY
                result = worker.find();
            }
        }
        return result;
    }

    protected boolean isTerm(X c) {
        return !isAny(c);
    }

    protected boolean isAny(X c) {
        return c == null || cxtInf.ANY.equals(c);
    }

    public class Worker_S_ANY_ANY {
        /** The components of the match-pattern */
        protected X ms, mp, mo;

        // The cache of tuples that were inferred for a given subject
        protected Cache<X, Set<X>> seenTypesCache = CacheFactory.createCache(10_000);
        // protected Cache<X, Set<X>> seenOutPredicatesCache = CacheFactory.createCache(10_000);

        // Cache for the set of properties between an (s, o) pair.
        protected Cache<Tuple2<X>, Set<X>> seenLinksCache = CacheFactory.createCache(100_000);

        // Cache of seen objects (not literals) and whether it appears as a subject.
        // Affects whether to emit range types for (_ p o) based on the ranges of p.
        protected Cache<X, Boolean> seenObjectResourcesCache = CacheFactory.createCache(100_000);

        protected Set<X> inPredicateCands;  // Candidate properties from which range types can be derived.
        protected Set<X> outPredicateCands; // Candidate properties from which domain types can be derived.
        public long enumerationThreshold;

        public Worker_S_ANY_ANY(X ms) {
            super();
            this.ms = ms;

            inPredicateCands = setup.getPropertyRanges().keySet();
            outPredicateCands = setup.getPropertyDomains().keySet();
            enumerationThreshold = (long)(inPredicateCands.size() * enumerationThresholdFactor);
        }

        public Stream<T> find() {
            // Special case: Objects can become subjects due to range declarations:
            // (o a R) <- (? p o) (p rdfs:range R)

            Iterator<T> infIt = null;
            if (setup.hasRangeDeclarations()) {
                // The special case only needs to be handled if the subject is bound:
                // If the subject is ANY, then this worker will use find(ANY, ANY, ANY) and
                // all objects will be encountered and the range inferences will be emitted then.
                // This only affects rdf:type triples inferred from range declarations.
                if (isTerm(ms) && !getMapper().isLiteral(ms) && (isAny(mp) || cxtInf.rdfType.equals(mp))) {
                    Set<X> seenTypes = seenTypesCache.get(ms, k -> new HashSet<>());
                    Set<X> seenInPredicates = getInPredicatesOfInstance(ms); // Calls .match(ANY, ANY, x).

                    // Collect all range types from the incomping properties' super properties.
                    Set<X> allSuperPs = accSuperProperties(seenInPredicates);

                    Set<X> newInfTypes = new HashSet<>();
                    // FIXME Add ranges of super properties
                    for (X inP : allSuperPs) {
                        Set<X> rangeTypes = setup.getRange(inP);
                        newInfTypes = accTypes(newInfTypes, rangeTypes, seenTypes);
                    }

                    Iterator<T> rangeTypesIt = Iter.map(newInfTypes.iterator(),
                            rt -> dstCreate(ms, cxtInf.rdfType, rt));

                    infIt = getOrConcat(infIt, rangeTypesIt);
                }
            }

            // Note: Using Iter.flatMap because Stream.flatMap.iterator
            // seems to result in non-streaming iterators at least on some JVMs.
            Iterator<T> subjectBasedIt = Iter.flatMap(backend.match(ms, mp, mo).iterator(), this::inf);
            Iterator<T> finalIt = getOrConcat(infIt, subjectBasedIt);
            return Iter.asStream(finalIt);
        }

        /** Resulting iterator does not include the input tuple if it is known to already have been produced. */
        protected Iterator<T> inf(T tuple) {
            X s = getMapper().subject(tuple);
            X p = getMapper().predicate(tuple);
            X o = getMapper().object(tuple);

            Set<X> seenTypes = seenTypesCache.get(s, k -> new HashSet<>());

            // Newly inferred types derived from this triple
            Set<X> newInfTypes = null;

            Set<X> superPropertiesInc; // We may have p subPropertyOf rdf:type
            Iterator<T> inferences = null;

            // Expansion from rdfs:subPropertyOf
            if (setup.hasPropertyDeclarations()) {
                superPropertiesInc = expandOrSelf(p, setup::getSuperPropertiesInc);
                // The inferences here do not include rdf:type as a super property.
                // We first collect all types and then emit them at the end.
                inferences = withSuperProperties(inferences, superPropertiesInc, s, p, o);
            } else {
                superPropertiesInc = Collections.singleton(p);
            }

            // Expansion for incoming predicates based on rdfs:range (based on the subject)
            if (setup.hasRangeDeclarations()) {
                // Base on the current p in (s, p, o) infer (o rdf:type T)
                 // inferences = withRangeTypesForObject(inferences, s, p, o);

//                for (X superP : superPropertiesInc) {
//                    Set<X> rangeTypes = setup.getRange(superP);
//                    newInfTypes = accTypes(newInfTypes, rangeTypes, seenTypes);
//                }
                inferences = withRangeTypesForObject(inferences, s, superPropertiesInc, o);
            }

            // if (setup.hasClassDeclarations()) {
                // Expansion for rdf:type based on rdfs:subPropertyOf
                if (superPropertiesInc.contains(cxtInf.rdfType) && !seenTypes.contains(o)) {
                    newInfTypes = accTypes(newInfTypes, o, seenTypes);
                }
            // }

            if (setup.hasDomainDeclarations()) {
                // Expansion for any newly seen predicate based on domain of the property
                for (X superP : superPropertiesInc) {
                    Set<X> domainTypes = setup.getDomain(superP);
                    newInfTypes = accTypes(newInfTypes, domainTypes, seenTypes);
                }
            }

            inferences = withTypeInfs(inferences, s, newInfTypes);

            // Suppress rdf:type triples when the type has already been seen
            boolean isSuppressedTriple = cxtInf.rdfType.equals(p) && seenTypes.contains(o);

            Iterator<T> result;
            if (isSuppressedTriple) {
                result = inferences == null ? Iter.empty() : inferences;
            } else {
                Iterator<T> self = Iter.of(tuple);
                result = inferences == null
                    ? self
                    : Iter.concat(self, inferences);
            }

            return result;
        }

        /** Accumulate super properties for a set of properties. */
        protected Set<X> accSuperProperties(Set<X> ps) {
            Set<X> allSuperPs = new LinkedHashSet<>();
            for (X p : ps) {
                if (!allSuperPs.contains(p)) {
                    allSuperPs.add(p);
                    Set<X> superPs = setup.getSuperProperties(p);
                    allSuperPs.addAll(superPs);
                }
            }
            return allSuperPs;
        }

        protected Iterator<T> withTypeInfs(Iterator<T> result, X s, Set<X> newTypes) {
            if (newTypes != null) {
                result = getOrConcat(result, Iter.iter(newTypes).map(t -> dstCreate(s, cxtInf.rdfType, t)));
            }
            return result;
        }

        // Produce inference tuples for the super properties of p.
        // rdf:type in the set of superPropertiesInc is ignored here.
        public Iterator<T> withSuperProperties(Iterator<T> inferences, Set<X> superPropertiesInc, X s, X p, X o) {
            // Infer super properties (s p2 o) <- (s p1 o) (p1 subPropertyOf p2)

            // If there are super properties of p (that are not p)...
            if (!(superPropertiesInc.size() == 1 && superPropertiesInc.contains(p))) {

                // Omit rdf:type from the super properties.
                if (superPropertiesInc.contains(cxtInf.rdfType)) {
                    superPropertiesInc = new HashSet<>(superPropertiesInc);
                    superPropertiesInc.remove(cxtInf.rdfType);
                }

                // There are super properties of p (other than p itself).
                Set<X> seenLinks = seenLinksCache.get(TupleFactory.create2(s, o), k -> new HashSet<>());
                if (!seenLinks.contains(p)) {
                    seenLinks.add(p); // Marking this property seen suppresses inference of the current triple; we will output it at the end
                    Set<X> newlyInferredPreds = addAndGetNew(null, seenLinks, superPropertiesInc);
                    if (newlyInferredPreds != null) {
                        inferences = getOrConcat(inferences,
                              Iter.iter(newlyInferredPreds).map(p2 -> dstCreate(s, p2, o)));
                    }
                }
            }
            return inferences;
        }

        public Iterator<T> withRangeTypesForObject(Iterator<T> inferences, X s, Set<X> ps, X o) {
            // The object may not appear as a subject so this might be the last time that we see it.
            // In the rare case that o == s and alwaysFetchRangeTypesBySubject the work was already done.
            if (isAny(ms) || (Objects.equals(o, s) && emitObjectTypesEagerly)) {
                if (!getMapper().isLiteral(o)) {
                    // FIXME Can't cache because the inference on o depends on p.
                    // boolean seenObject = seenObjectResourcesCache.getIfPresent(o) != null;
                    // if (!seenObject) {
                        // Object not handled yet (or we forgot because it was evicted from cache)
                        boolean emitObjectRangeTypesNow = true;
                        if (!emitObjectTypesEagerly) {
                            // If the object appears as a subject we don't have to produce the inferences now
                            boolean appearsAsSubject = backend.contains(o, cxtInf.ANY, cxtInf.ANY);
                            emitObjectRangeTypesNow = !appearsAsSubject;
                        }

                        if (emitObjectRangeTypesNow) {
                            // FIXME We need to get all superPropertiesInc of p and then their ranges.

                            Set<X> seenObjectTypes = seenTypesCache.get(o, k -> new HashSet<>());
                            Set<X> newInfOTypes = new HashSet<>();
                            for (X p : ps) {
                                Set<X> rangeTypes = setup.getRange(p);
                                if (!rangeTypes.isEmpty()) {
                                    Set<X> newlyInferredObjectTypes = accTypes(newInfOTypes, rangeTypes, seenObjectTypes);
                                    inferences = withTypeInfs(inferences, o, newlyInferredObjectTypes);
                                }
                            }
                        }
                    // }
                    // Create or refresh cache entry.
                    // seenObjectResourcesCache.put(o, Boolean.TRUE);
                }
            }
            return inferences;
        }

        /** Derive yet unseen types from incoming properties of the given subject. */
        public Set<X> accRangeTypesForSubject(Set<X> newInfTypes, X s, Set<X> seenTypes) {
            Set<X> seenInPredicates = getInPredicatesOfInstance(s);
            for (X inP : seenInPredicates) {
                Set<X> rangeTypes = setup.getRange(inP);
                newInfTypes = accTypes(newInfTypes, rangeTypes, seenTypes);
            }
            return newInfTypes;
        }


        /**
         * Return the set of ingoing properties of s. Unless s is a type then return the empty set.
         * XXX This should be toggleable behavior.
         */
        protected Set<X> getInPredicatesOfInstance(X s) {
            Set<X> result;
            if (backend.contains(cxtInf.ANY, cxtInf.rdfType, s)) {
                // Do not fetch incoming predicates for things that are probably classes - i.e. x which appear as ?_type_x
                // Classes may have millions+ incoming properties
                result = Collections.emptySet();
            } else {
                result = getInRangePredicates(s);
            }
            return result;
        }

        protected Set<X> getInRangePredicates(X s) {
            Set<X> result = getPredicates(backend, getMapper(), s, false, cxtInf.ANY, enumerationThreshold, inPredicateCands);
            return result;
        }

        protected Set<X> getOutPredicates(X s) {
            Set<X> result = getPredicates(backend, getMapper(), s, true, cxtInf.ANY, enumerationThreshold, outPredicateCands);
            return result;
        }

        protected Iterator<T> scan_ANY_ANY_O(X o) {
            Iterator<T> it = Iter.ofStream(backend.match(cxtInf.ANY, cxtInf.ANY, o));
            it = Iter.flatMap(it, MatchRDFSReduced.this::infSuperPropertyTuples);
            it = Iter.distinctReduced(it, 100_000);
            return it;
        }

        protected Iterator<T> find_ANY_ANY_O(X o) {
            if (!isTerm(o)) {
                throw new IllegalArgumentException("o must be concrete.");
            }

            // FIXME: uniqueSubjectsOf_ANY_type_T scans all subProperties of rdf:type
            //        BUT we lose the original property e.g. my:directType .
            // TODO We could have unique subjects find all (?s a subClass(o)) tuples
            //      I.e. don't scan type o when dealing with subClasses

            // This produces (?s a t) triples only for s with sub types of t.
            // So (s a o) does not exist
            // FIXME If the class hierarchy has a cycle we actually DO have to filter (s a o)!
            Iterator<T> instancesIt = Iter.map(uniqueSubjectsOf_ANY_type_T(o, false),
                    s -> dstCreate(s, cxtInf.rdfType, o));

            // TODO We need to inf domain triples for any s!
            Iterator<T> linksWithSuperPropertiesIt = scan_ANY_ANY_O(o);

            // Expand with super properties
            // Set<X> inPsClosure = accSuperProperties(inPs);

            // Iterator<T> linksIt = distinctPoTuplesWithSuperTypes(cxtInf.ANY, inPs, o);

            // Enrich with all super properties
            // Iterator<T> linksWithSuperPropertiesIt = Iter.flatMap(linksIt, MatchRDFSReduced.this::infSuperPropertyTuples);

            return Iter.concat(instancesIt, linksWithSuperPropertiesIt);
        }


        // Get all subjects related to o:
        // - Get type patterns using find_ANY_type_T for (? a o)
        // - Add all physical properties with inferred super properties
        // FIXME We can still leverage this method for (p, o) case where we have a fixed set of predicates!
        protected Iterator<T> find_ANY_ANY_O_OLD(X o) {
            if (!isTerm(o)) {
                throw new IllegalArgumentException("o must be concrete.");
            }

            // FIXME: uniqueSubjectsOf_ANY_type_T scans all subProperties of rdf:type
            //        BUT we lose the original property e.g. my:directType .
            // TODO We could have unique subjects find all (?s a subClass(o)) tuples
            //      I.e. don't scan type o when dealing with subClasses
            Iterator<T> instancesIt = Iter.map(uniqueSubjectsOf_ANY_type_T(o, true), s -> dstCreate(s, cxtInf.rdfType, o));

            // Get all incoming predicates.
            // FIXME This is broken! getInPredicates only checks for properties with range definitions.
            //       We would need a full sample of all properties - which requires scan.
            // So we need to scan ANY ANY o and just filter out duplicates.
            //
            Set<X> inPs = getInRangePredicates(o);
            inPs.remove(cxtInf.rdfType);

            // Expand with super properties
            // Set<X> inPsClosure = accSuperProperties(inPs);

            Iterator<T> linksIt = distinctPoTuplesWithSuperTypes(cxtInf.ANY, inPs, o);

            // Enrich with all super properties
            Iterator<T> linksWithSuperPropertiesIt = Iter.flatMap(linksIt, MatchRDFSReduced.this::infSuperPropertyTuples);

            return Iter.concat(instancesIt, linksWithSuperPropertiesIt);
        }
    }

    protected Iterator<T> infSuperPropertyTuples(T t) {
        X st = getMapper().subject(t);
        X pt = getMapper().predicate(t);
        X ot = getMapper().object(t);
        Set<X> superPs = expandOrSelf(pt, setup::getSuperPropertiesInc);
        return Iter.map(superPs.iterator(), pp -> dstCreate(st, pp, ot));
    }

    protected Set<X> accTypes(Set<X> result, X directType, Set<X> seenTypes) {
        return accTypes(result, Collections.singleton(directType), seenTypes);
    }

    /**
     * Adds the closure of any direct type to the set of seen types.
     * The result is the set of types that haven't been seen before.
     */
    protected Set<X> accTypes(Set<X> result, Set<X> directTypes, Set<X> seenTypes) {
        for (X directType : directTypes) {
            if (!seenTypes.contains(directType)) {
                result = addAndGetNew(result, seenTypes, directType);
                Set<X> typeClosure = setup.getSuperClasses(directType);
                result = addAndGetNew(result, seenTypes, typeClosure);
            }
        }
        return result;
    }

    /** Whether the given property is equal to or a sub-property of rdf:type. */
    protected boolean isEffectiveRdfType(X p) {
        boolean result = false;
        if (cxtInf.rdfType.equals(p)) {
            result = true;
        } else if (setup.hasPropertyDeclarations()) {
            Set<X> superProperties = setup.getSuperProperties(p);
            result = superProperties.contains(cxtInf.rdfType);
        }
        return result;
    }

    protected Set<Match1<X, T>> matchPatterns(Set<X> ps, Set<X> os) {
        return ps.stream().flatMap(pp -> os.stream().map(oo -> new Match1S<X, T>(pp, oo)))
                .collect(Collectors.toUnmodifiableSet());
    }

//    protected Iterator<X> distinctSubjects(Set<Tuple2<X>> patterns) {
//        return Iter.map(uniqueNodes(patterns), getMapper()::subject);
//    }


    /**
     * Execute to match distinct subjects.
     * There are two ways to execute this:
     * - concat multiple lookups
     * - scan and filter.
     */
    protected Iterator<X> uniqueNodes(Collection<Match1<X, T>> patterns) {
        Set<Match1<X, T>> nonEmptyStreams = new HashSet<>();
        Iterator<X> result = null;
        Iterator<T> contrib = null;
        try {
            for (Match1<X, T> pattern : patterns) {
                // Get subjects of the given predicate.
                contrib = Iter.ofStream(pattern.exec(backend));

                MapperX<X, T> mapper = backend.getMapper();
                // Filter matches of a (non-empty) contribution by those produced by previous ones.
                if (contrib.hasNext()) {
                    Iterator<X> contribX = Iter.flatMap(contrib, tuple -> pattern.project(mapper, tuple).iterator());
                    if (pattern.duplicates()) {
                        // FIXME Use reduced or filter at the end?
                        contribX = Iter.distinct(contribX);
                    }
                    if (!nonEmptyStreams.isEmpty()) {
                        // Do not emit x if will be emitted by a previous p.
                        Set<Match1<X, T>> filter = new HashSet<>(nonEmptyStreams);
                        contribX = Iter.filter(contribX, x -> {
                            return Iter.noneMatch(filter.iterator(),
                                    prevMatch -> prevMatch.contains(backend, x));
                        });
                    }
                    nonEmptyStreams.add(pattern);

                    result = getOrConcat(result, contribX);
                    contrib = null;
                }
            }
        } catch (RuntimeException e) {
            try {
                if (contrib != null) { Iter.close(contrib); }
            } finally {
                if (result != null) { Iter.close(contrib); }
            }
        }

        if (result == null) {
            result = Iter.empty();
        }

        return result;
    }

    protected Iterator<T> distinctPoTuples(X s, X pattern, X o) {
        Set<X> ps = Set.of(pattern);
        return distinctPoTuples(s, ps, o);
    }

    // FIXME - Include super properties - unless restricted by a given property!
    protected Iterator<T> distinctPoTuples(X s, Set<X> patterns, X o) {
        Set<X> nonEmptyStreams = new HashSet<>();
        Iterator<T> result = null;
        Iterator<T> contrib = null;
        try {
            for (X pattern : patterns) {

                // Get subjects of the given predicate.
                contrib = Iter.ofStream(backend.match(s, pattern, o));



                // Filter matches of a (non-empty) contribution by those produced by previous ones.
                if (contrib.hasNext()) {
                    if (!nonEmptyStreams.isEmpty()) {
                        // Do not emit x if will be emitted by a previous p.
                        Set<X> filter = new HashSet<>(nonEmptyStreams);
                        contrib = Iter.filter(contrib, tuple -> Iter.noneMatch(filter.iterator(),
                            prevPattern -> backend.contains(getMapper().subject(tuple), prevPattern, getMapper().object(tuple))));
                        nonEmptyStreams.add(pattern);
                    }

                    result = getOrConcat(result, contrib);
                    contrib = null;
                }
            }
        } catch (RuntimeException e) {
            try {
                if (contrib != null) { Iter.close(contrib); }
            } finally {
                if (result != null) { Iter.close(contrib); }
            }
        }

        if (result == null) {
            result = Iter.empty();
        }

        return result;
    }


    Set<X> getSuperPropertyConflicts(X p, Collection<X> conflictCandidates) {
        Set<X> pClosure = expandOrSelf(p, setup::getSuperPropertiesInc);
        Set<X> result = new LinkedHashSet<>();
        for (X c : conflictCandidates) {
            Set<X> cClosure = expandOrSelf(c, setup::getSuperPropertiesInc);
            Set<X> overlap = SetUtils.intersection(pClosure, cClosure);
            if (!overlap.isEmpty()) {
                result.add(c);
            }
        }
        return result;
    }

    // ONLY for a known set of properties - not for p == ANY
    protected Iterator<T> distinctPoTuplesWithSuperTypes(X s, Set<X> ps, X o) {
        Set<X> psWithData = new HashSet<>();
        Iterator<T> result = null;
        Iterator<T> contrib = null;
        try {
            for (X pattern : ps) {

                // Get subjects of the given predicate.
                contrib = Iter.ofStream(backend.match(s, pattern, o));

                // Filter matches of a (non-empty) contribution by those produced by previous ones.
                if (contrib.hasNext()) {
                    if (!psWithData.isEmpty()) {
                        Set<X> conflictCandidates = getSuperPropertyConflicts(pattern, psWithData);
                        if (!conflictCandidates.isEmpty()) {
                            // Do not emit x if will be emitted by a previous p.
                            Set<X> filter = new HashSet<>(psWithData);
                            contrib = Iter.filter(contrib, tuple -> Iter.noneMatch(filter.iterator(),
                                    prevPattern -> backend.contains(getMapper().subject(tuple), prevPattern, getMapper().object(tuple))));
                                psWithData.add(pattern);
                        }
                    }

                    result = getOrConcat(result, contrib);
                    contrib = null;
                }
            }
        } catch (RuntimeException e) {
            try {
                if (contrib != null) { Iter.close(contrib); }
            } finally {
                if (result != null) { Iter.close(contrib); }
            }
        }

        if (result == null) {
            result = Iter.empty();
        }

        return result;
    }

    /**
     *
     * @param typePs The concrete[!] set of effective rdf:type predicates.
     *           i.e. subProperties are not expanded.
     * @param o
     * @return
     */

    /**
     * @param typePs Set containing rdfType and all its subProperties.
     * @param o
     * @param includeO If false then do not create a matcher for (rdf:type o).
     *        Useful to optimize for ANY_ANY_O which will encounter all rdf:type triples.
     * @return
     */
    protected List<Match1<X, T>> matchPatterns_type_T(Set<X> typePs, X o, boolean includeO) {
        // Process o as a class.
        Set<X> subClasses = expandOrSelf(o, setup::getSubClassesInc);

        Set<X> extraPsByRange = subClasses.stream()
                .flatMap(cls -> setup.getPropertiesByRange(cls).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<X> extraPsByDomain = subClasses.stream()
                .flatMap(cls -> setup.getPropertiesByDomain(cls).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Expand the sets with sub-properties
        Set<X> extraPsByRangeAll = extraPsByRange.stream()
                .flatMap(p -> expandOrSelf(p, setup::getSubPropertiesInc).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<X> extraPsByDomainAll = extraPsByDomain.stream()
                .flatMap(p -> expandOrSelf(p, setup::getSubPropertiesInc).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Factor out properties that have the same class 'o' for domain and range.
        // Avoids having to iterate it twice.
        Set<X> psWithDomainAndRange = new LinkedHashSet<>(SetUtils.intersection(extraPsByDomain, extraPsByRange));
        extraPsByRange.removeAll(psWithDomainAndRange);
        extraPsByDomain.removeAll(psWithDomainAndRange);

        // Filter the sets based on the available data
        List<X> presentTypePs = typePs.stream().filter(pp -> backend.contains(cxtInf.ANY, pp, cxtInf.ANY)).toList();
        List<X> presentSubClasses = subClasses.stream().filter(oo -> backend.contains(cxtInf.ANY, cxtInf.ANY, oo)).toList();

        // Build lookups with the cartesian product of typePs X subclasses(o)
        // if includeO is false then exclude (rdf:type o tuples) lookups -
        // we will get those from a scan ANY_ANY_O
        List<Match1<X, T>> subClassMatchers = presentTypePs.stream()
                .flatMap(pp -> presentSubClasses.stream()
                .filter(oo -> includeO || !(cxtInf.rdfType.equals(pp) && oo.equals(o)))
                .<Match1<X,T>>map(oo -> new Match1S<X, T>(pp, oo)))
                .toList();

        List<Match1<X, T>> domainMatchers = extraPsByDomainAll.stream().map(pp -> (Match1<X, T>)new Match1S<X, T>(pp, null)).toList();
        List<Match1<X, T>> rangeMatchers = extraPsByRangeAll.stream().map(pp -> (Match1<X, T>)new Match1O<X, T>(null, pp)).toList();
        List<Match1<X, T>> domainAndRangeMatchers = psWithDomainAndRange.stream().map(pp -> (Match1<X, T>)new Match1SO<X, T>(pp)).toList();

        List<Match1<X, T>> result = new ArrayList<>();
        result.addAll(subClassMatchers);
        result.addAll(domainMatchers);
        result.addAll(rangeMatchers);
        result.addAll(domainAndRangeMatchers);
        return result;
    }

    protected Iterator<X> uniqueSubjectsOf_ANY_type_T(X o, boolean includeO) {
        Set<X> typePs = expandOrSelf(cxtInf.rdfType, setup::getSubPropertiesInc);
                // Set.of(cxtInf.rdfType);

        List<Match1<X, T>> matchPatterns = matchPatterns_type_T(typePs, o, includeO);
        return uniqueNodes(matchPatterns);
    }

    protected Iterator<T> find_S_P_ANY(X s, X p) {
        Set<X> subPs = expandOrSelf(p, setup::getSubPropertiesInc);
        Iterator<T> linkIt = distinctPoTuplesWithSuperTypes(s, subPs, cxtInf.ANY);
        return Iter.map(linkIt, t -> dstCreate(getMapper().subject(t), p, getMapper().object(t)));
    }


    protected Iterator<T> find_ANY_P_ANY(X p) {
        return find_X_notType_X(cxtInf.ANY, p, cxtInf.ANY);
    }

    protected Iterator<T> find_X_notType_X(X s, X p, X o) {
        if (!isTerm(p)) {
            throw new IllegalArgumentException("p must be a term");
        }

        Set<X> subPs = expandOrSelf(p, setup::getSubPropertiesInc);
        Iterator<T> it = distinctPoTuplesWithSuperTypes(s, subPs, o);

        // Iterator<T> it = Iter.ofStream(backend.match(cxtInf.ANY, p, cxtInf.ANY));
        Iterator<T> result = Iter.map(it, t -> {
            X ss = getMapper().subject(t);
            // X pp = getMapper().predicate(t);
            X oo = getMapper().object(t);
            return dstCreate(ss, p, oo);
//            Set<X> superPs = setup.getSuperProperties(pp);
//            if (superPs.isEmpty()) {
//                return Iter.of(t);
//            } else {
//                Iterator<T> superTuples = Iter.map(
//                    Iter.filter(superPs.iterator(), ppp -> !backend.contains(ss, ppp, oo)),
//                    pppp -> dstCreate(ss, pppp, oo));
//
//                return Iter.concat(Iter.of(t), superTuples);
//            }
        });
        return result;
    }

    protected List<Match1<X, T>> matchPatterns_P_O(Set<X> inPs, X o) {
        Set<X> rdfTypePs = expandOrSelf(cxtInf.rdfType, setup::getSubPropertiesInc);

        // Split present properties into those that are (subPropertiesOf) rdf:type and those that are not.
        // FIXME if (setup.hasClassDeclarations()) {
        Set<X> typePs = new LinkedHashSet<>();
        Set<X> nonTypePs = new LinkedHashSet<>();
        for (X inP : inPs) {
            if (rdfTypePs.contains(inP)) {
                typePs.add(inP);
            } else {
                nonTypePs.add(inP);
            }
        }

        List<Match1<X, T>> matchPatterns = new ArrayList<>();
        if (!typePs.isEmpty()) {
            List<Match1<X, T>> contrib = matchPatterns_type_T(typePs, o, true);
            matchPatterns.addAll(contrib);
        }

        if (!nonTypePs.isEmpty()) {
            Set<X> os = Set.of(o);
            Set<Match1<X, T>> contrib = matchPatterns(nonTypePs, os);
            matchPatterns.addAll(contrib);
        }
        return matchPatterns;
    }

    public Iterator<X> uniqueSubjectsOf_P_O(Set<X> inPs, X o) {
        List<Match1<X, T>> matchPatterns = matchPatterns_P_O(inPs, o);
        Iterator<X> result = uniqueNodes(matchPatterns);
        return result;
    }

    // FIXME - bugged
    protected Iterator<T> find_ANY_P_O(X p, X o) {
        if (!isTerm(o)) {
            throw new IllegalArgumentException("o must be concrete.");
        }

        Iterator<T> result;
        if (isEffectiveRdfType(p)) {
            Iterator<X> instancesIt = uniqueSubjectsOf_ANY_type_T(o, true);
            result = Iter.map(instancesIt, s -> dstCreate(s, cxtInf.rdfType, o));
        } else {
            result = distinctPoTuples(cxtInf.ANY, p, o);
        }
        return result;

    }

    /** Expander should be the reflexive 'Inc' version so that x is included in the expansion. */
    protected Set<X> expandOrSelf(X x, Function<X, Set<X>> expander) {
        Set<X> result = expander.apply(x);
        if (result.isEmpty()) {
            result = Set.of(x);
        }
        return result;
    }

    private static <T> Set<T> addAndGetNew(Set<T> acc, Set<T> base, T addition) {
        return addAndGetNew(acc, base, Collections.singleton(addition));
    }

    /**
     * Add every item in 'additions' that is not in 'base' both to 'base' and 'acc'.
     * If there is a change and 'acc' is null then a fresh linked hash set is allocated.
     * Returns the latest state of 'acc'.
     */
    private static <T> Set<T> addAndGetNew(Set<T> acc, Set<T> base, Set<T> additions) {
        Set<T> result = acc;
        List<T> newItems = new ArrayList<>(SetUtils.difference(additions, base));
        base.addAll(newItems);
        if (!newItems.isEmpty()) {
            if (result == null) {
                result = new LinkedHashSet<>();
            }
            result.addAll(newItems);
        }
        return result;
    }

    /**
     * Generic method to get a listing of a resource's predicates w.r.t. a given set of relevant predicates.
     * Starts off with iterating the resource's triples (in or out). Once more then 'threshold' triples are seen that way,
     * the presence of the remaining predicates in 'enumeration' is checked directly with contains checks.
     */
    public static <C, D> Set<C> getPredicates(
            Match<C, D> backend,
            MapperX<C, D> mapper,
            C s, boolean isForward, C any, long enumerationThreshold, Set<C> enumeration) {
        Set<C> result = new LinkedHashSet<>();
        boolean seenAll = false;

        // The maximum number of predicates we can expect based on the ontology
        int maxSeeableSize = enumeration.size();

        if (enumerationThreshold > 0) {
            Iterator<C> it = (isForward
                        ? backend.match(s, any, any)
                        : backend.match(any, any, s))
                    .map(mapper::predicate)
                    .iterator();

            long counter = 0;
            try {
                boolean aborted = false;
                while (it.hasNext()) {
                    C p = it.next();
                    result.add(p);
                    ++counter;
                    if (counter > enumerationThreshold) {
                        aborted = true;
                        break;
                    }
                    if (result.size() >= maxSeeableSize) {
                        // This case may trigger for ontologies with very few range declarations
                        seenAll = true;
                        break;
                    }
                }
                seenAll = !aborted;
            } finally {
                Iter.close(it);
            }
        }

        if (!seenAll) {
            // Don't re-check predicates which we have already seen
            Set<C> remainingCands = new HashSet<>(SetUtils.difference(enumeration, result));
            for (C candP : remainingCands) {
                boolean isPresent = isForward
                        ? backend.contains(s, candP, any)
                        : backend.contains(any, candP, s);
                if (isPresent) {
                    result.add(candP);
                }
            }
        }
        return result;
    }

    /**
     * Implementation of MatchRDFS as a wrapper over another Match source.
     */
    private static class InfFindTuple<X, T>
        extends MatchRDFS<X, T>
    {
        private final Match<X, T> base;

        public InfFindTuple(ConfigRDFS<X> setup, MapperX<X, T> mapper, Match<X, T> backend) {
            super(setup, mapper);
            this.base = backend;
        }

        @Override
        public Stream<T> sourceFind(X s, X p, X o) {
            return base.match(s,p,o);
        }

        @Override
        protected boolean sourceContains(X s, X p, X o) {
            try (Stream<T> stream = base.match(s, p, o)) {
                return stream.findFirst().isPresent();
            }
        }

        @Override
        public T dstCreate(X s, X p, X o) {
            T tuple = base.getTupleMapper().create(s, p, o);
            return tuple;
        }
    }

    private static <T> Iterator<T> getOrConcat(Iterator<T> base, Iterator<T> toAdd) {
        Iterator<T> result;
        if (toAdd == null) {
            result = base;
        } else {
            if (base == null) {
                result = toAdd;
            } else {
                IteratorConcat<T> it;
                if (base instanceof IteratorConcat) {
                    it = (IteratorConcat<T>)base;
                } else {
                    it = new IteratorConcat<>();
                    it.add(base);
                }
                it.add(toAdd);
                result = it;
            }
        }
        return result;
    }

    protected T dstCreate(X s, X p, X o) {
        return tupleMapper.create(s, p, o);
    }
}

/*
 * Match1: View a set of triples as a (multi-)set of nodes w.r.t. a Match source.
 *         Match1 instances can check for whether they contain a node.
 * Match1 instances effectively specify a single triple pattern and its projected component(s).
 */

interface Match1<X, T> {
    Stream<T> exec(Match<X, T> match);
    Stream<X> project(MapperX<X, T> mapper, T tuple);
    boolean contains(Match<X, T> match, X x);
    // Whether duplicates may be produced.
    boolean duplicates();
}

class Match1S<X, T> implements Match1<X, T> {
    private X p, o;

    public Match1S(X p, X o) { this.p = p; this.o = o; }

    @Override public Stream<T> exec(Match<X, T> match) {
        return match.match(null, p, o);
    }

    @Override public Stream<X> project(MapperX<X, T> mapper, T tuple) { return Stream.of(mapper.subject(tuple)); }
    @Override public boolean duplicates() { return p == null || o == null; }

    @Override
    public boolean contains(Match<X, T> match, X x) {
        return match.contains(x, p, o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(o, p);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Match1S<?, ?> other = (Match1S<?, ?>) obj;
        return Objects.equals(o, other.o) && Objects.equals(p, other.p);
    }

    @Override
    public String toString() {
        return "Match1S [p=" + p + ", o=" + o + "]";
    }
}

class Match1O<X, T> implements Match1<X, T> {
    private X s, p;
    public Match1O(X s, X p) { this.s = s; this.p = p; }

    @Override public Stream<T> exec(Match<X, T> match) {
        return match.match(s, p, null);
    }

    @Override public Stream<X> project(MapperX<X, T> mapper, T tuple) { return Stream.of(mapper.object(tuple)); }
    @Override public boolean duplicates() { return s == null || p == null; }

    @Override
    public boolean contains(Match<X, T> match, X x) {
        return match.contains(s, p, x);
    }

    @Override
    public int hashCode() {
        return Objects.hash(p, s);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Match1O<?, ?> other = (Match1O<?, ?>) obj;
        return Objects.equals(p, other.p) && Objects.equals(s, other.s);
    }

    @Override
    public String toString() {
        return "Match1O [s=" + s + ", p=" + p + "]";
    }
}

// Project S and O
class Match1SO<X, T> implements Match1<X, T> {
    private X p;
    public Match1SO(X p) { this.p = p; }

    @Override public Stream<T> exec(Match<X, T> match) {
        return match.match(null, p, null);
    }

    @Override public Stream<X> project(MapperX<X, T> mapper, T tuple) {
        X s = mapper.subject(tuple);
        X o = mapper.object(tuple);
        Stream<X> result = Objects.equals(s, o) ? Stream.of(s) : Stream.of(s, o);
        return result;
    }

    @Override public boolean duplicates() { return true; }

    @Override
    public boolean contains(Match<X, T> match, X x) {
        return match.contains(x, p, null) || match.contains(null, p, x);
    }

    @Override
    public int hashCode() {
        return Objects.hash(p);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Match1SO<?, ?> other = (Match1SO<?, ?>) obj;
        return Objects.equals(p, other.p);
    }

    @Override
    public String toString() {
        return "Match1SO [p=" + p + "]";
    }
}

