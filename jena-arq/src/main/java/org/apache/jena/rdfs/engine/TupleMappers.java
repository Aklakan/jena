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

package org.apache.jena.rdfs.engine;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

public class TupleMappers {
    private static TupleMapper3<Node, Triple> mapperSingletonTriple = new TupleMapperTriple();
    private static TupleMapper4<Node, Quad> mapperSingletonQuad = new TupleMapperQuad();

    public static TupleMapper3<Node, Triple> mapperTriple() { return mapperSingletonTriple; }
    public static TupleMapper4<Node, Quad> mapperQuad() { return mapperSingletonQuad; }

    private static class TupleMapperTriple implements TupleMapper3<Node, Triple> {
        @Override public Triple create(Node s, Node p, Node o) { return Triple.create(s, p, o); }
    }

    private static class TupleMapperQuad implements TupleMapper4<Node, Quad> {
        @Override public Quad create(Node g, Node s, Node p, Node o) { return Quad.create(g, s, p, o); }
    }
}
