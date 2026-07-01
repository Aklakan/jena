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

package org.apache.jena.tdb2.solver.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.index.RangeIndex;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarAlloc;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterDistinct;
import org.apache.jena.sparql.engine.iterator.QueryIterFilterExpr;
import org.apache.jena.sparql.engine.iterator.QueryIterGroup;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.iterator.QueryIterProject;
import org.apache.jena.sparql.engine.join.JoinKey;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.aggregate.AggAvgDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;
import org.apache.jena.sparql.expr.aggregate.AggGroupConcatDistinct;
import org.apache.jena.sparql.expr.aggregate.AggMax;
import org.apache.jena.sparql.expr.aggregate.AggMedianDistinct;
import org.apache.jena.sparql.expr.aggregate.AggMin;
import org.apache.jena.sparql.expr.aggregate.AggModeDistinct;
import org.apache.jena.sparql.expr.aggregate.AggSample;
import org.apache.jena.sparql.expr.aggregate.AggSampleDistinct;
import org.apache.jena.sparql.expr.aggregate.AggSumDistinct;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.expr.aggregate.AggregatorFactory;
import org.apache.jena.tdb2.solver.BindingNodeId;
import org.apache.jena.tdb2.solver.BindingTDB;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.NodeIdFactory;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.store.tupletable.TupleIndex;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;
import org.apache.jena.tdb2.store.tupletable.TupleTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpExecutorTDB2SkipScan {
    private static final Logger logger = LoggerFactory.getLogger(OpExecutorTDB2SkipScan.class);

    /** Must only be called if hasNeededColumns returned true. */
    @SuppressWarnings("unchecked")
    public static IndexMatch computeIndexMatch(TupleMap tm, TuplePatternSpec patternSpec) {
        Node[] quad = patternSpec.tuple();
        boolean canDoDirectDistinct = true;

        // An index is only usable if covers all needed slots of the query tuple.
        // boolean[] uncoveredSlots = new boolean[queryLen];
        int uncoveredSlotsMask = patternSpec.neededSlotsMask();

        ValueFilter<Node>[] residualConditions = null;
        int numResidualConditions = 0;

        int i;
        int idxLen = tm.length();
        for (i = 0; i < idxLen; ++i) {
            int tupleSlot = tm.mapIdx(i);
            Node node = quad[tupleSlot];
            if (!node.isConcrete()) {
                break;
            }
            uncoveredSlotsMask &= ~(1 << tupleSlot);
        }
        int keyLen = i;

        // The next slots of the index must map to the projection slots
        // Any constant not matched by HEAD is considered projected and filtered on.
        int j; // additional needed value slots
        for (j = keyLen; j < idxLen && uncoveredSlotsMask != 0; ++j) {
            int tupleSlot = tm.mapIdx(j);
            Node node = quad[tupleSlot];
            if (node.isVariable()) {
                Var v = Var.alloc(node);
                uncoveredSlotsMask &= ~(1 << tupleSlot);

                // Consider: SELECT DISTINCT ?g ?p { GRAPH ?g { ?s :p ?s } }
                // PGSO and PGOS are suitable indices
                // If we pick PGOS we would evaluate the quality group for the S slot
                // So we test restriction based on the order of the original slot indices
                // S comes before O, so we test S = O instead of O = S
                // The important part is just: all members of equality groups must be covered.

                boolean isProjected = patternSpec.projection().contains(v);

                // If the variable in not projected, then we need to skip over it
                // This breaks simple distinct
                if (!isProjected) {
                    canDoDirectDistinct = false;
                }
            } else {
                if (residualConditions == null) {
                    residualConditions = new ValueFilter[idxLen - j];
                }

                // Add the constant node as a filter condition.
                residualConditions[numResidualConditions++] = new ValueFilter<>(j, node);
            }
        }
        int valLen = j - keyLen;

        if (uncoveredSlotsMask != 0) {
            return null;
        }

        return new IndexMatch(canDoDirectDistinct, residualConditions, numResidualConditions, keyLen, valLen);
    }

    /** Resolve the {@link NodeTupleTable} for a pattern of the given tuple length. */
    private static NodeTupleTable resolveTable(int tupleLength, ExecutionContext execCxt) {
        if (tupleLength == 3) {
            GraphTDB graph = (GraphTDB)execCxt.getActiveGraph();
            return graph.getNodeTupleTable();
        } else if (tupleLength == 4) {
            DatasetGraphTDB ds = (DatasetGraphTDB)execCxt.getDataset();
            return ds.getQuadTable().getNodeTupleTable();
        } else {
            throw new IllegalStateException("Unexpected tuple length: " + tupleLength);
        }
    }

    public static QueryIterator tryExec(PatternQuery patternQuery, QueryIterator input, ExecutionContext execCxt) {
        QueryIterator qIter = null;
        Node[] testTuple = patternQuery.tuple();
        NodeTupleTable table = resolveTable(testTuple.length, execCxt);

        qIter = QueryIter.flatMap(input, b -> {
            PatternQuery subst = PatternQuery.substitute(patternQuery, b);
            JoinKey proj = subst.project();
            Node[] tuple = subst.tuple();
            TuplePatternSpec lookup = TuplePatternSpec.create(tuple, proj);
            return tryExec(table, patternQuery.distinct(), lookup, execCxt);
        }, execCxt);
        return qIter;
    }

    public static QueryIterator tryExec(NodeTupleTable nodeTupleTable, boolean distinct, TuplePatternSpec lookup, ExecutionContext execCxt) {
        List<SkipScanCandidate> candidates = planCandidates(nodeTupleTable, lookup);
        SkipScanCandidate best = pickBest(candidates);

        // In the end we must build a tuple with the constants
        if (logger.isDebugEnabled()) {
            logger.debug("Best matching index: " + (best == null ? null : best.index()));
        }

        if (best == null) {
            return null;
        }
        return execCandidate(nodeTupleTable, distinct, lookup, best, execCxt);
    }

    /**
     * Enumerate the usable indexes for the given lookup. Each returned candidate carries
     * the matched index, its {@link IndexMatch}, and the variable order the index produces.
     * This is the planning phase: it does not touch the node table or execute anything, so
     * callers may inspect candidate orders and choose a combination before executing.
     */
    public static List<SkipScanCandidate> planCandidates(NodeTupleTable nodeTupleTable, TuplePatternSpec lookup) {
        TupleTable tupleTable = nodeTupleTable.getTupleTable();
        List<SkipScanCandidate> candidates = new ArrayList<>();

        for (TupleIndex tupleIndex : tupleTable.getIndexes()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Trying index: " + tupleIndex);
            }

            // Only consider indexes backed by a BPlusTree
            if (tupleIndex instanceof TupleIndexRecord recordIdx) {
                RangeIndex rangeIndex = recordIdx.getRangeIndex();
                if (!(rangeIndex instanceof BPlusTree)) {
                    continue;
                }
            }

            TupleMap tm = tupleIndex.getMapping();
            IndexMatch match = computeIndexMatch(tm, lookup);

            if (match == null) {
                // Index is unsuitable because it could not cover all required slots.
                continue;
            }

            List<Var> order = producedOrder(lookup, match, tm);
            candidates.add(new SkipScanCandidate(tupleIndex, match, order));
        }
        return candidates;
    }

    /** Choose the best candidate using {@link IndexMatch#COMPARATOR}, or null if none. */
    public static SkipScanCandidate pickBest(List<SkipScanCandidate> candidates) {
        SkipScanCandidate best = null;
        for (SkipScanCandidate candidate : candidates) {
            if (best == null || IndexMatch.COMPARATOR.compare(candidate.match(), best.match()) < 0) {
                best = candidate;
            }
        }
        return best;
    }

    /**
     * The variable order produced by an index for a given lookup: the projected variables
     * in index-slot order over the needed slots. Variables that participate in equality
     * links are omitted because their direct slot order is not externally observable.
     */
    private static List<Var> producedOrder(TuplePatternSpec lookup, IndexMatch match, TupleMap tm) {
        Node[] quad = lookup.tuple();
        int neededSlots = match.numNeededSlots();
        List<Var> order = new ArrayList<>(neededSlots);
        for (int i = 0; i < neededSlots; ++i) {
            int tupleSlot = tm.mapIdx(i);
            Node node = quad[tupleSlot];
            if (node.isVariable()) {
                Var v = Var.alloc(node);
                if (lookup.projection().contains(v) && !order.contains(v)) {
                    order.add(v);
                }
            }
        }
        return order;
    }

    /** Execute a previously planned candidate. */
    public static QueryIterator execCandidate(NodeTupleTable nodeTupleTable, boolean distinct, TuplePatternSpec lookup,
                                              SkipScanCandidate candidate, ExecutionContext execCxt) {
        NodeTable nodeTable = nodeTupleTable.getNodeTable();
        TupleIndex bestIndex = candidate.index();
        IndexMatch bestMatch = candidate.match();

        TupleMap tm = bestIndex.getMapping();
        IndexMap im = tryComputeIndexMap(lookup, bestMatch, tm, nodeTable);
        if (im == null) {
            // If null then at least one Node had no corresponding NodeId in the nodeTable.
            return new QueryIterNullIterator(execCxt);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("IndexMap Match Data: " + im);
            logger.debug("IndexMap Projection: " + Arrays.toString(im.proj()));
            logger.debug("IndexMap Conditions: " + Arrays.toString(im.residualConditions()));
            logger.debug("IndexMap Links: " + Arrays.toString(im.equalityLinks()));
            logger.debug("IndexMap Query Tuple: " + Arrays.asList(im.tuple()));
        }

        QueryIterator r = exec(bestIndex, nodeTable, lookup.projection(), im, execCxt);

        if (distinct && !bestMatch.canDoDirectDistinct()) {
            r = new QueryIterDistinct(r, null, execCxt);
        }
        return r;
    }

    /**
     * Actual execution.
     *
     * @param tupleIndex
     * @param nodeTable
     * @param projectVars
     * @param map
     * @param execCxt
     * @return
     */
    @SuppressWarnings("removal")
    public static QueryIterator exec(TupleIndex tupleIndex, NodeTable nodeTable, JoinKey projectVars, IndexMap map, ExecutionContext execCxt) {
        // Cast validity of the arguments is assumed to be ensured!
        TupleIndexRecord tupleIndexRecord = (TupleIndexRecord)tupleIndex;
        RangeIndex rangeIndex = tupleIndexRecord.getRangeIndex();
        BPlusTree bpt = (BPlusTree)rangeIndex;

        int n = map.tuple().length;
        Tuple<NodeId> pattern = TupleFactory.create(map.tuple());

        Tuple<NodeId> minPattern = NodeIdUtils.anyToMin(pattern);
        Tuple<NodeId> maxPattern = NodeIdUtils.anyToMax(pattern);

        RecordFactory rf = rangeIndex.getRecordFactory();
        Record minRecord = record(rf, minPattern);
        Record maxRecord = record(rf, maxPattern);

        // VarMap[] proj = map.proj;
        ValueFilter<NodeId>[] residualConditions = map.residualConditions();
        EqualityLink[] equalityLinks = map.equalityLinks();

        Iterator<BindingNodeId> bindingIdIt;

        VarMap[] finalProj;
        Expr filterExpr = null;
        if (equalityLinks == null) {
            finalProj = map.proj();
        } else {
            // For linked slots, such as {?s :p ?s} introduce dummy non-distinguished vars.
            // [?s :p ?s] -> {?s :p ?.foo} FILTER(?s = ?.foo)

            // The variables of the equalityLinks may not have been projected yet.
            // So we must compute a proper projection.

            VarMap[] finalProjTmp = new VarMap[map.proj().length + 2 * equalityLinks.length];
            System.arraycopy(map.proj(), 0, finalProjTmp, 0, map.proj().length);
            VarAlloc va = new VarAlloc(ARQConstants.allocVarAnonMarker + "X");
            int i = map.proj().length;
            Set<Var> seenVars = null;
            for (EqualityLink entry : equalityLinks) {
                int primaryIdx = entry.equalToIdx();
                Var primaryVar = (Var)map.nodeTuple()[primaryIdx];
                if (!projectVars.contains(primaryVar) && (seenVars == null || !seenVars.contains(primaryVar))) {
                    if (seenVars == null) {
                        seenVars = new HashSet<>();
                    }
                    seenVars.add(primaryVar);
                    finalProjTmp[i++] = new VarMap(primaryIdx, primaryVar);
                }

                int secondaryIdx = entry.idx();
                // Var v = (Var)map.nodeTuple[secondaryIdx]; -> sameAs primaryVar
                Var extraVar = va.allocVar();
                finalProjTmp[i++] = new VarMap(secondaryIdx, extraVar);

                Expr contrib = new E_Equals(new ExprVar(primaryVar), new ExprVar(extraVar));
                filterExpr = logicalAnd(filterExpr, contrib);
            }
            finalProj = Arrays.copyOf(finalProjTmp, i);
        }

        Iterator<Record> recordIt = bpt.distinctByKeyPrefix(n * NodeId.SIZE, minRecord, maxRecord);

        // Filter by recheck conditions first.
        if (residualConditions != null) {
            recordIt = Iter.filter(recordIt, record -> {
                execCxt.checkCancelSignal(); // TODO Rare case where deprecated checkCancelSignal actually makes sense?

                byte[] recordKey = record.getKey();
                for (int i = 0; i < residualConditions.length; ++i) {
                    ValueFilter<NodeId> condition = residualConditions[i];
                    int idxx = condition.idx();
                    NodeId expected = condition.value();
                    NodeId actual = extractNodeId(recordKey, idxx);
                    if (!expected.equals(actual)) {
                        return false;
                    }
                }
                return true;
            });
        }

        // Build the (initial) BindingNodeId
        bindingIdIt = Iter.map(recordIt, record -> {
            byte[] recordKey = record.getKey();
            BindingNodeId b = new BindingNodeId();
            for (VarMap varMap : finalProj) {
                NodeId nodeId = extractNodeId(recordKey, varMap.idx());
                b.put(varMap.var(), nodeId);
            }
            return b;
        });

        Iterator<Binding> bindingIt = Iter.map(bindingIdIt, bid -> new BindingTDB(bid, nodeTable));
        QueryIterator qIter = QueryIterPlainWrapper.create(bindingIt, execCxt);

        // Apply filters if needed.
        // Presence of filters also implies allocated variables that need projecting away.
        if (filterExpr != null) {
            qIter = new QueryIterFilterExpr(qIter, filterExpr, execCxt);

            // XXX QueryIterDistinguishedVars?
            qIter = QueryIterProject.create(qIter, projectVars, execCxt);
        }

        return qIter;
    }

    /**
     * Maps Nodes to NodeIds. Returns null if any constraint is unsatisfiable
     * due to lack of a corresponding node id.
     */
    public static IndexMap tryComputeIndexMap(TuplePatternSpec lookup, IndexMatch match, TupleMap tm, NodeTable nodeTable) {
        // Keep track of the highest needed slot of the index
        // int neededSlots = keyLen + valLen;
        Node[] quad = lookup.tuple();
        int neededSlots = match.numNeededSlots();

        Node[] nodeTuple = new Node[neededSlots]; // The query tuple reordered for the index.
        for (int i = 0; i <neededSlots; ++i) {
            int tupleSlot = tm.mapIdx(i);
            Node node = quad[tupleSlot];
            nodeTuple[i] = node;
        }

        NodeId[] tuple = new NodeId[neededSlots];

        int x;
        for (x = 0; x < neededSlots; ++x) {
            int tupleSlot = tm.mapIdx(x);
            Node node = quad[tupleSlot];
            if (!node.isConcrete()) {
                break;
            }
            NodeId nodeId = nodeTable.getNodeIdForNode(node);

            if (NodeId.isDoesNotExist(nodeId)) {
                // Empty result set
                return null;
            }

            tuple[x] = nodeId;
        }

        for (int y = x; y < neededSlots; ++y) {
            tuple[y] = NodeId.NodeIdAny;
        }

        ValueFilter<NodeId>[] mappedConditions = null;
        EqualityLink[] mappedLinks = null;
        // mappedLinks = mapLinks(match., numProjectSlots, tm);

        if (match.residualConditions() != null) {
            mappedConditions = mapValueConditions(match.residualConditions(), match.numResidualConditions(), tm, nodeTable);

            // Returns null if nodes could not be mapped
            if (mappedConditions == null) {
                // skip
                return null;
            }
        }

        if (lookup.equalityLinks() != null) {
            mappedLinks = mapEqualityLinks(lookup.equalityLinks(), lookup.equalityLinks().length, tm);
        }

        VarMap[] newProj = mapProjection(lookup, tm);
        return new IndexMap(nodeTuple, tuple, newProj, mappedConditions, mappedLinks);
    }

    private static Expr logicalAnd(Expr base, Expr contrib) {
        return base == null ? contrib : new E_LogicalAnd(base, contrib);
    }

    private static NodeId extractNodeId(byte[] bytes, int index) {
        NodeId nodeId = NodeIdFactory.get(bytes, index * NodeId.SIZE);
        return nodeId;
    }

    public static VarMap[] mapProjection(TuplePatternSpec lookup, TupleMap tupleMap) {
        int[] proj = lookup.projs();
        int n = proj.length;
        VarMap[] result = new VarMap[n];
        for (int i = 0; i < n; ++i) {
            int tupleIdx = proj[i];
            Node node = lookup.tuple()[tupleIdx];
            Var var = Var.alloc(node);
            int idx = tupleMap.unmapIdx(tupleIdx);
            result[i] = new VarMap(idx, var);
        }
        return result;
    }

    public static EqualityLink[] mapEqualityLinks(EqualityLink[] links, int n, TupleMap tupleMap) {
        EqualityLink[] result = new EqualityLink[n];
        for (int i = 0; i < n; ++i) {
            EqualityLink link = links[i];
            int primary = tupleMap.unmapIdx(link.idx());
            int secondary = tupleMap.unmapIdx(link.equalToIdx());
            result[i] = new EqualityLink(primary, secondary);
        }
        return result;
    }

    // returns null if any condition could not be mapped.
    public static ValueFilter<NodeId>[] mapValueConditions(ValueFilter<Node>[] conditions, int n, TupleMap tupleMap, NodeTable nodeTable) {
        @SuppressWarnings("unchecked")
        ValueFilter<NodeId>[] result = new ValueFilter[n];
        for (int i = 0; i < n; ++i) {
            ValueFilter<Node> condition = conditions[i];
            int slot = tupleMap.unmapIdx(condition.idx());
            NodeId nodeId = nodeTable.getNodeIdForNode(condition.value());
            if (nodeId == null) {
                return null;
            }
            result[i] = new ValueFilter<>(slot, nodeId);
        }
        return result;
    }

    /** Create a record directly from the tuple (without a tuple map) */
    private static Record record(RecordFactory factory, Tuple<NodeId> tuple) {
        int n = tuple.len();
        byte[] b = new byte[n * NodeId.SIZE];
        for (int i = 0; i < n ; i++) {
            NodeIdFactory.set(tuple.get(i), b, i * NodeId.SIZE);
        }
        return factory.create(b);
    }

    /** −−− Execution of OpDistinct −−− */

    public static QueryIterator tryExec(OpDistinct opDistinct, QueryIterator input, ExecutionContext execCxt) {
        PatternQuery patternQuery = PatternQuery.createOrNull(opDistinct);
        if (patternQuery != null) {
            QueryIterator qIter = OpExecutorTDB2SkipScan.tryExec(patternQuery, input, execCxt);
            if (qIter != null) {
                return qIter;
            }
        }
        return null;
    }

    /** −−− Execution of OpGroupBy −−− */

    public static QueryIterator tryExec(OpGroup opGroup, QueryIterator input, ExecutionContext execCxt) {
        // Only handle grouping by plain variables (no group-by expressions).
        if (!opGroup.getGroupVars().getExprs().isEmpty()) {
            return null;
        }

        List<ExprAggregator> aggregators = opGroup.getAggregators();
        if (aggregators.isEmpty()) {
            return null;
        }

        List<Var> groupVars = opGroup.getGroupVars().getVars();

        // Determine the set of value variables (one skip-scan per distinct value var).
        // All-or-nothing: every aggregator must be skip-scan-suitable (DISTINCT single-var,
        // or MIN/MAX/SAMPLE). Aggregators that share a value var share a single skip-scan so
        // each distinct value is emitted exactly once per group.
        List<Var> valueVars = new ArrayList<>();
        for (ExprAggregator eAgg : aggregators) {
            Var v = suitableVarOrNull(eAgg.getAggregator());
            if (v == null) {
                return null;
            }
            if (!valueVars.contains(v)) {
                valueVars.add(v);
            }
        }

        // Build one pattern query per distinct value var.
        List<PatternQuery> patternQueries = new ArrayList<>(valueVars.size());
        for (Var v : valueVars) {
            JoinKey newProj = JoinKey.newBuilder()
                .addAll(groupVars)
                .add(v)
                .build();
            Op newOp = new OpDistinct(new OpProject(opGroup.getSubOp(), newProj));
            PatternQuery patternQuery = PatternQuery.createOrNull(newOp);
            if (patternQuery == null) {
                return null;
            }
            patternQueries.add(patternQuery);
        }

        // Non-distinct aggregators - the skip-scan input already yields each value once per group.
        List<ExprAggregator> newAggs = new ArrayList<>(aggregators.size());
        for (ExprAggregator eAgg : aggregators) {
            newAggs.add(convertToNonDistinct(eAgg));
        }

        // The merged, grouped input is built per input binding. Index availability and the
        // achievable group-by order do not depend on the concrete substituted values, so a
        // static feasibility check is sufficient to decide whether to take over execution.
        if (!isMergeFeasible(patternQueries, groupVars, execCxt)) {
            return null;
        }

        QueryIterator merged = QueryIter.flatMap(input, b -> {
            QueryIterator combined = buildMergedForBinding(patternQueries, valueVars, groupVars, b, execCxt);
            return combined;
        }, execCxt);

        return new QueryIterGroup(merged, opGroup.getGroupVars(), newAggs, execCxt);
    }

    /**
     * Static feasibility: every aggregator's pattern must have at least one usable index,
     * and there must exist a single group-by variable order achievable by every aggregator
     * so the per-aggregator skip-scans can be combined with a single order-aware merge.
     */
    private static boolean isMergeFeasible(List<PatternQuery> patternQueries, List<Var> groupVars, ExecutionContext execCxt) {
        List<List<SkipScanCandidate>> perAgg = new ArrayList<>(patternQueries.size());
        for (PatternQuery pq : patternQueries) {
            NodeTupleTable table = resolveTable(pq.tuple().length, execCxt);
            TuplePatternSpec lookup = TuplePatternSpec.create(pq.tuple(), pq.project());
            List<SkipScanCandidate> candidates = planCandidates(table, lookup);
            if (candidates.isEmpty()) {
                return false;
            }
            perAgg.add(candidates);
        }
        return chooseCommonOrder(perAgg, groupVars) != null;
    }

    /**
     * Find a group-by variable order achievable by at least one candidate of every
     * aggregator. Returns that order (the group vars in a consistent sequence), or null.
     *
     * <p>Single-aggregator queries trivially succeed. For multiple aggregators, the leading
     * group-by variables must appear in the same sequence in some candidate of each
     * aggregator so prefix-based equal-group comparison in the merge is valid.</p>
     */
    private static List<Var> chooseCommonOrder(List<List<SkipScanCandidate>> perAgg, List<Var> groupVars) {
        Set<Var> groupVarSet = new HashSet<>(groupVars);
        // Candidate orders for the first aggregator, projected onto the group vars.
        for (SkipScanCandidate c0 : perAgg.get(0)) {
            List<Var> order = groupByOrder(c0, groupVarSet);
            if (order == null) {
                continue;
            }
            boolean okForAll = true;
            for (int i = 1; i < perAgg.size(); ++i) {
                if (!hasCandidateWithOrder(perAgg.get(i), groupVarSet, order)) {
                    okForAll = false;
                    break;
                }
            }
            if (okForAll) {
                return order;
            }
        }
        return null;
    }

    /**
     * The group-by variable order produced by a candidate: the candidate's produced order
     * restricted to the group vars. Returns null if not all group vars are covered (the
     * candidate cannot establish the required grouping order).
     */
    private static List<Var> groupByOrder(SkipScanCandidate candidate, Set<Var> groupVarSet) {
        List<Var> order = new ArrayList<>(groupVarSet.size());
        for (Var v : candidate.order()) {
            if (groupVarSet.contains(v) && !order.contains(v)) {
                order.add(v);
            }
        }
        return order.size() == groupVarSet.size() ? order : null;
    }

    private static boolean hasCandidateWithOrder(List<SkipScanCandidate> candidates, Set<Var> groupVarSet, List<Var> wanted) {
        for (SkipScanCandidate c : candidates) {
            List<Var> order = groupByOrder(c, groupVarSet);
            if (wanted.equals(order)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Build the merged, group-ordered iterator for a single input binding. Substitutes the
     * binding into each aggregator's pattern, executes the candidate matching the common
     * group-by order, and merges the resulting iterators. Returns an empty iterator if the
     * binding cannot be planned.
     */
    private static QueryIterator buildMergedForBinding(List<PatternQuery> patternQueries, List<Var> valueVars,
                                                       List<Var> groupVars, Binding b, ExecutionContext execCxt) {
        List<List<SkipScanCandidate>> perAgg = new ArrayList<>(patternQueries.size());
        List<TuplePatternSpec> lookups = new ArrayList<>(patternQueries.size());
        List<NodeTupleTable> tables = new ArrayList<>(patternQueries.size());
        List<Var> substValueVars = new ArrayList<>(patternQueries.size());

        for (int i = 0; i < patternQueries.size(); ++i) {
            PatternQuery subst = PatternQuery.substitute(patternQueries.get(i), b);
            NodeTupleTable table = resolveTable(subst.tuple().length, execCxt);
            TuplePatternSpec lookup = TuplePatternSpec.create(subst.tuple(), subst.project());
            List<SkipScanCandidate> candidates = planCandidates(table, lookup);
            if (candidates.isEmpty()) {
                return new QueryIterNullIterator(execCxt);
            }
            perAgg.add(candidates);
            lookups.add(lookup);
            tables.add(table);
            substValueVars.add(valueVars.get(i));
        }

        // The group vars that are still variable after substitution.
        List<Var> remainingGroupVars = new ArrayList<>(groupVars.size());
        for (Var v : groupVars) {
            if (!b.contains(v)) {
                remainingGroupVars.add(v);
            }
        }

        List<Var> commonOrder = chooseCommonOrder(perAgg, remainingGroupVars);
        if (commonOrder == null) {
            return new QueryIterNullIterator(execCxt);
        }

        List<QueryIterator> iters = new ArrayList<>(patternQueries.size());
        for (int i = 0; i < patternQueries.size(); ++i) {
            SkipScanCandidate chosen = pickCandidateForOrder(perAgg.get(i), new HashSet<>(remainingGroupVars), commonOrder);
            QueryIterator it = execCandidate(tables.get(i), patternQueries.get(i).distinct(), lookups.get(i), chosen, execCxt);
            iters.add(it);
        }

        if (iters.size() == 1) {
            return iters.get(0);
        }
        NodeTable nodeTable = tables.get(0).getNodeTable();
        return new QueryIterMergeScanGroup(iters, substValueVars, commonOrder, nodeTable, execCxt);
    }

    private static SkipScanCandidate pickCandidateForOrder(List<SkipScanCandidate> candidates, Set<Var> groupVarSet, List<Var> wanted) {
        for (SkipScanCandidate c : candidates) {
            if (wanted.equals(groupByOrder(c, groupVarSet))) {
                return c;
            }
        }
        // Should not happen: feasibility was checked. Fall back to best candidate.
        return pickBest(candidates);
    }

    /**
     * If the aggregator is suitable for skip-scan execution then return its single
     * value variable, else null.
     *
     * <p>An aggregator is suitable iff its result is invariant under removing duplicate
     * values of a single value expression that is a plain variable. This covers every
     * DISTINCT single-var aggregator (COUNT, SUM, AVG, MEDIAN, MODE, SAMPLE, GROUP_CONCAT)
     * plus the non-distinct MIN, MAX and SAMPLE aggregators (for which DISTINCT is
     * irrelevant).</p>
     */
    static Var suitableVarOrNull(Aggregator agg) {
        if (!isSkipScanSuitable(agg)) {
            return null;
        }
        return varOrNull(agg);
    }

    private static boolean isSkipScanSuitable(Aggregator agg) {
        return agg instanceof AggCountVarDistinct
            || agg instanceof AggSumDistinct
            || agg instanceof AggAvgDistinct
            || agg instanceof AggMedianDistinct
            || agg instanceof AggModeDistinct
            || agg instanceof AggSampleDistinct
            || agg instanceof AggGroupConcatDistinct
            // Non-distinct MIN/MAX/SAMPLE are distinct-invariant.
            || agg instanceof AggMin
            || agg instanceof AggMax
            || agg instanceof AggSample;
    }

    private static ExprAggregator convertToNonDistinct(ExprAggregator eAgg) {
        Aggregator newAgg = convertToNonDistinct(eAgg.getAggregator());
        return new ExprAggregator(eAgg.getVar(), newAgg);
    }

    /**
     * Convert a (possibly DISTINCT) skip-scan-suitable aggregator into its non-distinct
     * equivalent. Because the skip-scan input yields each value at most once per group,
     * the non-distinct aggregator computes the same result. MIN/MAX/SAMPLE (already
     * distinct-invariant) are returned unchanged.
     */
    private static Aggregator convertToNonDistinct(Aggregator agg) {
        Expr expr = agg.getExprList().get(0);
        if (agg instanceof AggCountVarDistinct) {
            return AggregatorFactory.createCountExpr(false, expr);
        } else if (agg instanceof AggSumDistinct) {
            return AggregatorFactory.createSum(false, expr);
        } else if (agg instanceof AggAvgDistinct) {
            return AggregatorFactory.createAvg(false, expr);
        } else if (agg instanceof AggMedianDistinct) {
            return AggregatorFactory.createMedian(false, expr);
        } else if (agg instanceof AggModeDistinct) {
            return AggregatorFactory.createMode(false, expr);
        } else if (agg instanceof AggSampleDistinct) {
            return AggregatorFactory.createSample(false, expr);
        } else if (agg instanceof AggGroupConcatDistinct acd) {
            return AggregatorFactory.createGroupConcat(false, expr, acd.getSeparator(), null);
        }
        // AggMin, AggMax, AggSample: already non-distinct and distinct-invariant.
        return agg;
    }

    /** Extract the first argument of the agg's exprList as a Var, or null. */
    private static Var varOrNull(Aggregator agg) {
        ExprList el = agg.getExprList();
        if (el == null || el.size() != 1) {
            return null;
        }
        Expr e = el.get(0);
        return e.isVariable() ? e.asVar() : null;
    }
}
