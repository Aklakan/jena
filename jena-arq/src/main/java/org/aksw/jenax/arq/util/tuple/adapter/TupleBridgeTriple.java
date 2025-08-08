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

package org.aksw.jenax.arq.util.tuple.adapter;

import org.aksw.commons.tuple.bridge.TupleBridge3;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class TupleBridgeTriple
    implements TupleBridge3<Triple, Node>
{
    public static final TupleBridgeTriple INSTANCE = new TupleBridgeTriple();

    public static TupleBridgeTriple get() {
        return INSTANCE;
    }

    @Override
    public int getDimension() {
        return 3;
    }

    @Override
    public Node get(Triple triple, int idx) {
        return getNode(triple, idx);
    }

    @Override
    public Triple build(Node s, Node p, Node o) {
        return Triple.create(s, p, o);
    }

    /** Access a triple's component by a zero-based index in order s, p, o.
     * Raises {@link IndexOutOfBoundsException} for any index outside of the range [0, 2]*/
    public static Node getNode(Triple triple, int idx) {
        switch (idx) {
        case 0: return triple.getSubject();
        case 1: return triple.getPredicate();
        case 2: return triple.getObject();
        default: throw new IndexOutOfBoundsException("Cannot access index " + idx + " of a triple");
        }
    }
}
