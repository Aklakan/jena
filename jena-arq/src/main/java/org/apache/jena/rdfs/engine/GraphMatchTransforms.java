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

import java.util.function.Function;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/** Static method to wrap graphs based on transformations on the {@link Match} level. */
public class GraphMatchTransforms {
    public static GraphTransform asGraphTransform(MatchTransform<Node, Triple> matchTransform) {
        return graph -> transform(graph, matchTransform);
    }

    /** High level transform on the Node/Triple domain level. */
    public static Graph transform(Graph graph, MatchTransform<Node, Triple> matchTransform) {
        Match<Node, Triple> baseMatch;

        if (graph instanceof GraphMatch gm && graph.getClass().equals(GraphMatch.class)) {
            // Unwrap a GraphMatch and transform the match-delegate.
            baseMatch = gm.getMatch();
        } else {
            // Create a match view over the graph.
            baseMatch = new MatchGraph(graph);
        }

        // Transform the base match and create a graph view on top of it.
        Match<Node, Triple> out = matchTransform.apply(baseMatch);
        Graph r = new GraphMatch(graph, out);
        return r;
    }

    /** Low level transform on some backing X/T model via mapping. */
    public static <X, T> Graph transform(Graph base, Function<Graph, Match<X, T>> graphToMatch, MapperX<X, T> mapper, MatchTransform<X, T> matchTransform) {
        Match<X, T> inMatch = graphToMatch.apply(base);
        Match<X, T> outMatch = matchTransform.apply(inMatch);
        Graph result = asGraph(base, outMatch, mapper);
        return result;
    }

    public static <X, T> Graph asGraph(Graph baseGraph, Match<X, T> inMatch, MapperX<X, T> mapper) {
        Match<Node, Triple> match = new MatchDomainView<>(inMatch, mapper);
        Graph result = new GraphMatch(baseGraph, match);
        return result;
    }
}
