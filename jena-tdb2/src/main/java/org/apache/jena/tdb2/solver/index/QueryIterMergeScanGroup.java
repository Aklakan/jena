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

import java.util.List;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.tdb2.solver.BindingTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.NodeIdFactory;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

/**
 * Order-aware merge of several skip-scan iterators that share the same group-by order.
 *
 * <p>Each input iterator produces distinct {@code (group-vars, valueVar)} bindings, ordered
 * by the supplied {@code groupVars} sequence (the index key order). This operator performs a
 * k-way merge by group key: for each distinct group it emits, in turn, every input's rows for
 * that group before advancing to the next group. The output is therefore contiguously grouped
 * by the group key, which is what {@link org.apache.jena.sparql.engine.iterator.QueryIterGroup}
 * requires. Each emitted binding carries the group vars plus only its own input's value var,
 * so a downstream group/aggregate computes each aggregate over exactly its own distinct values.</p>
 *
 * <p>Ordering is compared on the serialized {@link NodeId} bytes so that it matches the
 * underlying B+Tree key ordering (unsigned, big-endian), which is the order the inputs are
 * produced in. Group-var {@code NodeId}s are taken directly from {@link BindingTDB} when
 * possible, otherwise they are resolved from the {@link NodeTable} for the bound node, so the
 * comparison stays consistent with the index ordering even when the skip-scan wraps its
 * bindings (e.g. for equality-link filtering and projection).</p>
 */
public class QueryIterMergeScanGroup extends QueryIter {

    private final List<QueryIterator> inputs;
    private final List<Var> valueVars;
    private final Var[] groupVars;
    private final NodeTable nodeTable;

    // The current head (peeked) binding of each input, or null if that input is exhausted.
    private final Binding[] heads;

    // Iteration state for emitting one group across all inputs.
    private int activeInput = -1;     // input currently being drained for the current group
    private byte[][] currentGroupKey; // serialized group key of the current group, or null

    public QueryIterMergeScanGroup(List<QueryIterator> inputs, List<Var> valueVars, List<Var> groupVars,
                                   NodeTable nodeTable, ExecutionContext execCxt) {
        super(execCxt);
        if (inputs.size() != valueVars.size()) {
            throw new IllegalArgumentException("inputs and valueVars must have the same size");
        }
        this.inputs = inputs;
        this.valueVars = valueVars;
        this.groupVars = groupVars.toArray(new Var[0]);
        this.nodeTable = nodeTable;
        this.heads = new Binding[inputs.size()];
        for (int i = 0; i < inputs.size(); ++i) {
            advance(i);
        }
    }

    private void advance(int i) {
        QueryIterator it = inputs.get(i);
        heads[i] = it.hasNext() ? it.nextBinding() : null;
    }

    @Override
    protected boolean hasNextBinding() {
        if (activeInput >= 0) {
            // Still draining inputs for the current group.
            return true;
        }
        for (Binding head : heads) {
            if (head != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Binding moveToNextBinding() {
        if (activeInput < 0) {
            // Start a new group: find the smallest group key among the input heads.
            currentGroupKey = smallestGroupKey();
            activeInput = nextActiveInputForCurrentGroup(-1);
            if (activeInput < 0) {
                throw new IllegalStateException("moveToNextBinding called with no available rows");
            }
        }

        // Emit the current head of activeInput (which belongs to currentGroupKey).
        Binding row = heads[activeInput];
        advance(activeInput);

        // If the active input has no more rows for this group, move to the next input.
        if (!hasRowForCurrentGroup(activeInput)) {
            activeInput = nextActiveInputForCurrentGroup(activeInput);
            if (activeInput < 0) {
                // Group fully emitted across all inputs.
                currentGroupKey = null;
            }
        }
        return row;
    }

    /** Compute the lexicographically-smallest group key among all non-exhausted heads. */
    private byte[][] smallestGroupKey() {
        byte[][] best = null;
        for (int i = 0; i < heads.length; ++i) {
            if (heads[i] == null) {
                continue;
            }
            byte[][] key = groupKey(heads[i]);
            if (best == null || compareKeys(key, best) < 0) {
                best = key;
            }
        }
        return best;
    }

    /** Index of the next input (strictly after {@code from}) whose head belongs to the current group. */
    private int nextActiveInputForCurrentGroup(int from) {
        for (int i = from + 1; i < heads.length; ++i) {
            if (hasRowForCurrentGroup(i)) {
                return i;
            }
        }
        return -1;
    }

    private boolean hasRowForCurrentGroup(int i) {
        Binding head = heads[i];
        if (head == null) {
            return false;
        }
        return compareKeys(groupKey(head), currentGroupKey) == 0;
    }

    /** Serialize the group vars of a binding into per-var NodeId byte arrays. */
    private byte[][] groupKey(Binding binding) {
        byte[][] key = new byte[groupVars.length][];
        for (int i = 0; i < groupVars.length; ++i) {
            NodeId id = nodeIdOf(binding, groupVars[i]);
            byte[] bytes = new byte[NodeId.SIZE];
            // null NodeId should not occur for group vars; encode as zero to keep a total order.
            if (id != null) {
                NodeIdFactory.set(id, bytes, 0);
            }
            key[i] = bytes;
        }
        return key;
    }

    /**
     * The {@link NodeId} of a variable in a binding. Prefers the {@link BindingTDB} NodeId to
     * avoid materializing nodes; otherwise resolves the bound node via the node table so the
     * ordering remains consistent with the index.
     */
    private NodeId nodeIdOf(Binding binding, Var var) {
        if (binding instanceof BindingTDB b) {
            NodeId id = b.getNodeId(var);
            if (id != null) {
                return id;
            }
        }
        Node n = binding.get(var);
        return n == null ? null : nodeTable.getNodeIdForNode(n);
    }

    private static int compareKeys(byte[][] a, byte[][] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; ++i) {
            int c = Bytes.compare(a[i], b[i]);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    @Override
    protected void requestCancel() {
        for (QueryIterator it : inputs) {
            it.cancel();
        }
    }

    @Override
    protected void closeIterator() {
        for (QueryIterator it : inputs) {
            it.close();
        }
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.println(Lib.className(this));
    }
}
