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

package org.apache.jena.geosparql.access;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Objects;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.geosparql.implementation.vocabulary.Geo;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.system.G;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Central place for accessing spatial objects in a {@link Graph} */
public class AccessGeoSparql {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** True iff the graph contains geosparql geometries. */
    public static boolean hasGeometries(Graph graph) {
        return hasOrIsGeometry(graph, null);
    }

    /** True iff the node has geosparql geometries. Arguments must not be null. */
    public static boolean isGeometryByProperties(Graph graph, Node boundNode) {
        Objects.requireNonNull(boundNode);
        return hasOrIsGeometry(graph, boundNode);
    }

    /** True if the node has a geometry or default geometry. Arguments must not be null. */
    public static boolean isFeatureByProperties(Graph graph, Node boundNode) {
        Objects.requireNonNull(boundNode);
        boolean result =
            graph.contains(boundNode, Geo.HAS_DEFAULT_GEOMETRY_NODE, null) ||
            graph.contains(boundNode, Geo.HAS_GEOMETRY_NODE, null);
        return result;
    }

    /** True if the node is a geosparql spatial object. Arguments must not be null. Wgs84 does not count. */
    public static boolean isSpatialObjectByProperties(Graph graph, Node boundNode) {
        return isGeometryByProperties(graph, boundNode) || isFeatureByProperties(graph, boundNode);
    }

    /**
     * Find all triples with geo:hasDefaultGeometry and geo:hasGeometry predicates.
     * If a feature has a default geometry, then this method will omit all its (non-default) geometries.
     */
    public static ExtendedIterator<Triple> findSpecificGeoResources(Graph graph) {
        // List resources that have a default geometry followed by those that
        // only have a non-default one.
        ExtendedIterator<Triple> it1 = graph.find(null, Geo.HAS_DEFAULT_GEOMETRY_NODE, null);
        ExtendedIterator<Triple> it2 = graph.find(null, Geo.HAS_GEOMETRY_NODE, null);
        try {
            boolean hasDefaultGeometry = it1.hasNext();
            // No default geometry -> no need to filter.
            if (hasDefaultGeometry) {
                it2 = it2.filterDrop(t -> G.hasProperty(graph, t.getSubject(), Geo.HAS_DEFAULT_GEOMETRY_NODE));
            }
        } catch (Throwable t) {
            throw handleExceptionAndClose(t, it1, it2);
        }
        return it1.andThen(it2);
    }

    public static ExtendedIterator<Triple> findSpecificGeoResources(Graph graph, Node feature) {
        Objects.requireNonNull(feature);
        ExtendedIterator<Triple> result = graph.find(feature, Geo.HAS_DEFAULT_GEOMETRY_NODE, null);
        try {
            if (!result.hasNext()) {
                result.close();
            }
            result = graph.find(feature, Geo.HAS_GEOMETRY_NODE, null);
        } catch (Throwable t) {
            result.close();
            throw new RuntimeException(t);
        }
        return result;
    }

    /**
     * Resolve a feature to its set of specific geometries via the following chain:
     * <pre>
     *   feature -&gt; (geo:hasDefaultGeometry, geo:hasGeometry) -&gt;
     *     ({geo:asWKT, geo:asGML}, geo:hasSerialization) -&gt; geo-literal.
     * </pre>
     *
     * If a geo:hasDefaultGeometry does not lead to a valid geo-literal there is no backtracking to geo:hasGeometry.
     */
    public static Iterator<Triple> findSpecificGeoLiteralsByFeature(Graph graph, Node feature) {
        return Iter.flatMap(findSpecificGeoResources(graph, feature),
            t -> findSpecificGeoLiterals(graph, t.getObject()));
    }

    /**
     * Iterate all triples of geometry resources with their most specific serialization form.
     * The specific properties geo:asWKT and geo:asGML take precedence over the more general geo:hasSerialization.
     * This means if a resource has wkt and/or gml then all geo:hasSerialization triples will be omitted for it.
     */
    public static ExtendedIterator<Triple> findSpecificGeoLiterals(Graph graph) {
        ExtendedIterator<Triple> it1 = graph.find(null, Geo.AS_WKT_NODE, null);
        ExtendedIterator<Triple> it2 = graph.find(null, Geo.AS_GML_NODE, null);
        ExtendedIterator<Triple> result = it1.andThen(it2);
        try {
            // If there is no specific serialization property use the general one.
            if (!result.hasNext()) {
                result.close();
                result = graph.find(null, Geo.HAS_SERIALIZATION_NODE, null);
            } else {
                // Append more general serializations for those resources that lack a specific one.
                ExtendedIterator<Triple> it3 = graph.find(null, Geo.HAS_SERIALIZATION_NODE, null);
                it3 = it3.filterDrop(t ->
                    G.hasProperty(graph, t.getSubject(), Geo.AS_WKT_NODE) ||
                    G.hasProperty(graph, t.getSubject(), Geo.AS_GML_NODE));
                result = result.andThen(it3);
            }
        } catch (Throwable t) {
            result.close();
            throw new RuntimeException(t);
        }
        return result;
    }

    /**
     * Iterate a given geometry resource's most specific geometry literals.
     * The geometry resource must not be null.
     * A specific serialization (WKT, GML) takes precedence over the more general hasSerialization property.
     */
    public static Iterator<Triple> findSpecificGeoLiterals(Graph graph, Node geometry) {
        Objects.requireNonNull(geometry);
        Iterator<Triple> wktNodeIter = graph.find(geometry, Geo.AS_WKT_NODE, null);
        Iterator<Triple> gmlNodeIter = graph.find(geometry, Geo.AS_GML_NODE, null);
        Iterator<Triple> iter = Iter.append(wktNodeIter, gmlNodeIter);
        try {
            if (!iter.hasNext()) {
                Iter.close(iter);
                // Fallback to the more generic property.
                iter = graph.find(geometry, Geo.HAS_SERIALIZATION_NODE, null);
            }
        } catch (Throwable t) {
            Iter.close(iter);
            throw new RuntimeException(t);
        }
        return iter;
    }

    public static ExtendedIterator<Triple> findSpatialTriplesByProperties(Graph graph) {
        ExtendedIterator<Triple> it1 = findSpecificGeoResources(graph);
        ExtendedIterator<Triple> it2 = findSpecificGeoLiterals(graph);
        // Note: No distinct applied here because features and geometries are assumed to be disjoint sets.
        return it1.andThen(it2);
    }

    public static Node getGeometry(Graph graph, Node geometry) {
        Triple t = getGeometryTriple(graph, geometry);
        Node n = (t == null) ? null : t.getObject();
        return n;
    }

    public static Triple getGeometryTriple(Graph graph, Node geometry) {
        Objects.requireNonNull(geometry);

        // Find the geometry literal of the geometry resource.
        Triple t;
        if ((t = getTripleSP(graph, geometry, Geo.HAS_SERIALIZATION_NODE)) != null) {
            return t;
        }

        // If hasSerialization not found then check asWKT.
        if ((t = getTripleSP(graph, geometry, Geo.AS_WKT_NODE)) != null) {
            return t;
        }

        // If asWKT not found then check asGML.
        if ((t = getTripleSP(graph, geometry, Geo.AS_GML_NODE)) != null) {
            return t;
        }

        return null;
    }

    private static Triple getTripleSP(Graph graph, Node s, Node p) {
        Node o = G.getSP(graph, s, p);
        Triple t = (o == null) ? null : Triple.create(s, p, o);
        return t;
    }

    /** Shared code to test whether a node or graph has serialization properties. */
    private static boolean hasOrIsGeometry(Graph graph, Node boundNode) {
        boolean result =
            graph.contains(boundNode, Geo.HAS_SERIALIZATION_NODE, null) ||
            graph.contains(boundNode, Geo.AS_WKT_NODE, null) ||
            graph.contains(boundNode, Geo.AS_GML_NODE, null);
        return result;
    }

    /**
     * Close the iterators and return a post-processed exception.
     * Returns a QueryCancelledException as-is and wraps any other exception as a RuntimeException.
     */
    private static RuntimeException handleExceptionAndClose(Throwable t, Iterator<?> ... iterators) {
        for (Iterator<?> it : iterators) {
            try {
                Iter.close(it);
            } catch (Exception e) {
                LOGGER.warn("Error during close.", e);
            }
        }

        if (t instanceof QueryCancelledException e) {
            return e;
        }

        return new RuntimeException(t);
    }
}
