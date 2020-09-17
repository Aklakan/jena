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

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleTableCore2;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.util.NodeUtils;


/**
 * Decorator for TripleTableCore whose add/delete methods also sync a set (typically a LinkedHashSet)
 * Invocation of .find() with only placeholders yields a stream from that set instead
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class TripleTableCore2
    extends TupleTableCore2<Triple, Node, TripleTableCore>
    implements TripleTableCore
{
    public TripleTableCore2(TripleTableCore primary, TripleTableCore secondary) {
        super(primary, secondary);
    }

    @Override
    public Stream<Triple> find(Node s, Node p, Node o) {
        boolean matchesAny = NodeUtils.isNullOrAny(s) && NodeUtils.isNullOrAny(p) && NodeUtils.isNullOrAny(o);
        Stream<Triple> result = matchesAny
                ? secondary.find(s, p, o)
                : primary.find(s, p, o);
        return result;
    }
}
