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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.geosparql.implementation.GeometryWrapper;
import org.apache.jena.geosparql.implementation.vocabulary.Geo;
import org.apache.jena.geosparql.implementation.vocabulary.SpatialExtension;
import org.apache.jena.geosparql.spatial.ConvertLatLon;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.system.G;
import org.apache.jena.system.RDFDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessWgs84 {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** True iff the graph contains (point) geometries modeled with the wgs84 vocabulary. */
    public static boolean hasGeometries(Graph graph) {
        boolean result =
            graph.contains(null, SpatialExtension.GEO_LAT_NODE, null) ||
            graph.contains(null, SpatialExtension.GEO_LON_NODE, null);
        return result;
    }

    /** For each matching resource, build triples of format 's geo:hasGeometry geometryLiteral'. */
    // XXX geo:hasSerialization might seem a better choice but the original jena-geosparql implementation used geo:hasGeometry.
    public static Iterator<Triple> findWgs84PseudoTriples(Graph graph, Node s) {
        return findWgs84PseudoTriples(graph, s, Geo.HAS_GEOMETRY_NODE);
    }

    /** For each matching resource, build pseudo triples of format 's p geometryLiteral'. */
    public static Iterator<Triple> findWgs84PseudoTriples(Graph graph, Node s, Node p) {
        return Iter.map(findWgs84Geometries(graph, s), e -> Triple.create(e.getKey(), p, e.getValue().asNode()));
    }

    /** For each matching resource, build geometry literals from the cartesian product of the WGS84 lat/long properties. */
    public static Iterator<Entry<Node, GeometryWrapper>> findWgs84Geometries(Graph graph, Node s) {
        // Warn about multiple lat/lon combinations only at most once per graph.
        boolean enableWarnings = false;
        boolean[] loggedMultipleLatLons = { false };
        Iterator<Triple> latIt = graph.find(s, SpatialExtension.GEO_LAT_NODE, Node.ANY);
        IteratorCloseable<Entry<Node, GeometryWrapper>> result = Iter.iter(latIt).flatMap(triple -> {
            Node feature = triple.getSubject();
            Node lat = triple.getObject();

            // Create the cross-product between lats and lons.
            Iterator<Node> lons = G.iterSP(graph, feature, SpatialExtension.GEO_LON_NODE);

            // On malformed data this can cause lots of log output. Perhaps it's better to keep validation separate from indexing.
            int[] lonCounter = {0};
            Iterator<Entry<Node, GeometryWrapper>> r = Iter.iter(lons).map(lon -> {
                if (enableWarnings) {
                    if (lonCounter[0] == 1) {
                        if (!loggedMultipleLatLons[0]) {
                            LOGGER.warn("Geo predicates: multiple longitudes detected on feature " + feature + ". Further warnings will be omitted.");
                            loggedMultipleLatLons[0] = true;
                        }
                    }
                    ++lonCounter[0];
                }
                GeometryWrapper geometryWrapper = ConvertLatLon.toGeometryWrapper(lat, lon);
                return Map.entry(feature, geometryWrapper);
            });
            return r;
        });
        return result;
    }

    /**
     * Read lat/lon values for the given subject. Null if there are no such properties.
     * Throws {@link DatatypeFormatException} when detecting incorrect use of these properties.
     */
    public static Node getGeometry(Graph graph, Node s) {
        Node lat = null;
        try {
            lat = G.getZeroOrOneSP(graph, s, SpatialExtension.GEO_LAT_NODE);
        } catch (RDFDataException ex) {
            throw new DatatypeFormatException(s + " has more than one geo:lat property.");
        }

        Node lon = null;
        try {
            lon = G.getZeroOrOneSP(graph, s, SpatialExtension.GEO_LON_NODE);
        } catch ( RDFDataException ex) {
            throw new DatatypeFormatException(s + " has more than one geo:lon property.");
        }

        // Both null -> return null.
        if (lat == null && lon == null) {
            return null;
        }

        if (lat == null) {
            throw new DatatypeFormatException(s + " has a geo:lon property but is missing geo:lat.");
        }
        if (lon == null) {
            throw new DatatypeFormatException(s + " has a geo:lat property but is missing geo:lon.");
        }
        Node geometryLiteral = ConvertLatLon.toNode(lat, lon);
        return geometryLiteral;
    }
}
