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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

/**
 *
 * (1) Generates a balanced class hierarchy.
 *            C
 *       /         \
 *     C0           C1
 *    /  \         /  \
 * C00    C01   C10    C11

 * (2) For each leaf class, generate a corresponding specific properties based on the leaf class name.
 * (2a) Generate type triples with subject / object
 *   x00s rdf:type C00
 *   x00o rdf:type C00

 * domain, range or both:
 * E.g. x00s p01_d x00o
 * E.g. x00s p01_r x00o
 * E.g. x00s p01_dr x00o
 */
class BenchmarkRDFSDataGenerator {
    record Data(Model schema, Model data, Resource rootClass, Property rootProperty) {}

    private int maxClassHierarchyDepth;
    private int childrenPerClass;
    private int numInstancesPerLeafClass;

    protected BenchmarkRDFSDataGenerator() {}

    /**
     * For instance triples (s p o):
     * artifically add sub property chains (s pn o) where pn subProperty of p(n-1)
     */
    private int subPropertyChainLength;

    public BenchmarkRDFSDataGenerator setMaxClassHierarchyDepth(int maxClassHierarchyDepth) {
        this.maxClassHierarchyDepth = maxClassHierarchyDepth;
        return this;
    }

    public BenchmarkRDFSDataGenerator setChildrenPerClass(int childrenPerClass) {
        this.childrenPerClass = childrenPerClass;
        return this;
    }

    public BenchmarkRDFSDataGenerator setNumInstancesPerLeafClass(int numInstancesPerLeafClass) {
        this.numInstancesPerLeafClass = numInstancesPerLeafClass;
        return this;
    }

    public BenchmarkRDFSDataGenerator setSubPropertyChainLength(int subPropertyChainLength) {
        this.subPropertyChainLength = subPropertyChainLength;
        return this;
    }

    public Data generate() {
        Model schema = ModelFactory.createDefaultModel();
        Resource rootClass = schema.createResource("http://ex.org/C");
        Property rootProperty = schema.createProperty("http://ex.org/p");
        generateBalancedSubClassTree(rootClass, maxClassHierarchyDepth, childrenPerClass);

        Set<Resource> leafClasses = getLeafClasses(rootClass);

        Model data = ModelFactory.createDefaultModel();

        // For each leaf class generate properties.
        for (Resource leafClass : leafClasses) {
            for (boolean d : new boolean[] {false, true}) {
                for (boolean r : new boolean[] {false, true}) {
                    Property baseP = getPropertyForClass(leafClass, d, r);
                    baseP.addProperty(RDFS.subPropertyOf, rootProperty);
                    if (d) { baseP.addProperty(RDFS.domain, leafClass); }
                    if (r) { baseP.addProperty(RDFS.range, leafClass); }

                    // Add sub property chain.
                    Property p = baseP;
                    for (int i = 0; i < subPropertyChainLength; ++i) {
                        Property subP = schema.createProperty(p.getURI() + "_" + i);
                        subP.addProperty(RDFS.subPropertyOf, p);
                        p = subP;
                    }

                    for (int i = 0; i < numInstancesPerLeafClass; ++i) {
                        Resource s = getResourceForClass(data, leafClass, i, true);
                        s.addProperty(RDF.type, leafClass);

                        Resource o = getResourceForClass(data, leafClass, i, false);
                        o.addProperty(RDF.type, leafClass);

                        data.add(s, baseP, o);
                    }
                }
            }
        }

        if (false) {
            System.out.println("Data -----");
            RDFDataMgr.write(System.out, data, RDFFormat.TURTLE);
            System.out.println("Schema -----");
            RDFDataMgr.write(System.out, schema, RDFFormat.TURTLE);
            System.out.println("Leaf Classes: " + leafClasses);
        }
        return new Data(schema, data, rootClass, rootProperty);
    }


    /** Create (C0 rdfs:subClassOf C) ... (Cn rdfs:subClassOf C). */
    public static List<Resource> generateSubClasses(Resource parent, int n) {
        List<Resource> result = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            Resource child = parent.getModel().createResource(parent.getURI() + i);
            child.addProperty(RDFS.subClassOf, parent);
            result.add(child);
        }
        return result;
    }

    public static void generateBalancedSubClassTree(Resource root, int maxDepth, int childrenPerClass) {
        generateBalancedSubClassTree(root, maxDepth, childrenPerClass, 0);
    }

    private static void generateBalancedSubClassTree(Resource root, int maxDepth, int childrenPerClass, int currentDepth) {
        if (currentDepth < maxDepth) {
            List<Resource> children = generateSubClasses(root, childrenPerClass);
            children.forEach(c -> generateBalancedSubClassTree(c, maxDepth, childrenPerClass, currentDepth + 1));
        }
    }

    public static <C extends Collection<Resource>> C getLeafs(Resource root, Property p, boolean isChildToParent, C result, Set<Resource> visited) {
        // Could also base this code on RDFSFactory.
        if (!visited.contains(root)) {
            visited.add(root);
            List<Resource> children = isChildToParent
                ? root.getModel().listSubjectsWithProperty(p, root).toList()
                : root.getModel().listObjectsOfProperty(root, p).mapWith(RDFNode::asResource).toList();
            if (children.isEmpty()) {
                result.add(root);
            }
            for(Resource child : children) {
                getLeafs(child, p, isChildToParent, result, visited);
            }
        }
        return result;
    }

    public static Set<Resource> getLeafClasses(Resource root) {
        return getLeafs(root, RDFS.subClassOf, true, new LinkedHashSet<>(), new HashSet<>());
        // return new LinkedHashSet<>(ResourceUtils.maximalLowerElements(Set.of(root), RDFS.subClassOf, true));
    }

    public static Set<Resource> getLeafProperties(Property p) {
        return getLeafs(p, RDFS.subPropertyOf, true, new LinkedHashSet<>(), new HashSet<>());
    }

    public static Property getPropertyForClass(Resource c, boolean domain, boolean range) {
        return c.getModel().createProperty(
            c.getNameSpace() + c.getLocalName().replace('C', 'p')
            + (domain ? "d" : "")
            + (range ? "r" : ""));
    }

    /** Create a resource in 'model' based on the URI of 'c'. */
    public static Resource getResourceForClass(Model model, Resource c, int id, boolean isSubject) {
        return model.createResource(
            c.getNameSpace() + ( isSubject ? c.getLocalName().replace('C', 's') : c.getLocalName().replace('C', 'o'))
            + "_" + id);
    }

    public static BenchmarkRDFSDataGenerator create() {
        return new BenchmarkRDFSDataGenerator();
    }
}
