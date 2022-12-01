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

package org.apache.jena.riot.system;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.graph.PrefixMappingAdapter;
import org.junit.Assert;
import org.junit.Test;


public class TestPrefixMapStd {
    @Test
    public void testFirstPrefixWins() {
        PrefixMap pm = PrefixMapFactory.builder().setLastPrefixWins(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("a:x", actual);
    }

    @Test
    public void testLastLastWins() {
        PrefixMap pm = PrefixMapFactory.builder().setLastPrefixWins(true).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("b:x", actual);
    }

    @Test
    public void testFirstPrefixWinsLazy() {
        PrefixMap pm = PrefixMapFactory.builder().setMapFactory(LinkedHashMap::new).setEagerIriToPrefix(false).setLastPrefixWins(false).setUseLocking(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("a:x", actual);
    }

    @Test
    public void testLastLastWinsLazy() {
        PrefixMap pm = PrefixMapFactory.builder().setMapFactory(LinkedHashMap::new).setEagerIriToPrefix(false).setLastPrefixWins(true).setUseLocking(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("b:x", actual);
    }

    @Test
    public void testFastTrackWithTrie() {
        PrefixMap pm = PrefixMapFactory.builder().setFastTrackEnabled(true).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/foo");
        String actual = pm.abbreviate("http://example.org/foobar");
        Assert.assertEquals("a:foobar", actual);
    }

    @Test
    public void testTrieOnly() {
        PrefixMap pm = PrefixMapFactory.builder().setFastTrackEnabled(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/foo");
        String actual = pm.abbreviate("http://example.org/foobar");
        Assert.assertEquals("b:bar", actual);
    }

    @Test
    public void testFastTrackOnly() {
        PrefixMap pm = PrefixMapFactory.builder().setFastTrackEnabled(true).setTrieEnabled(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/foo");
        String actual = pm.abbreviate("http://example.org/foobar");
        Assert.assertEquals("a:foobar", actual);
    }

    public static void main(String[] args) {
        Model model = RDFDataMgr.loadModel("prefix.cc.2022-09-19.ttl");
        // Iterators.pa
        Map<String, String> map = model.getNsPrefixMap();
        List<String> prefixes = new ArrayList<>(map.keySet());
        Collections.sort(prefixes);
        Random random = new Random(0);
        Function<String, String> gen = randomCurieSupplier(random, prefixes, map);

        String queryStr = createQueryStr(map, gen);
        System.out.println(queryStr);


        Supplier<Query> queryFactory = () -> new Query(new Prologue(new PrefixMappingAdapter(PrefixMapFactory.builder().build())));
        Query query = queryFactory.get();

        QueryFactory.parse(query, queryStr, null, Syntax.syntaxARQ);


        System.out.println(query);


    }

    public static String createQueryStr(Map<String, String> map, Function<String, String> gen) {
        StringBuilder sb = new StringBuilder();
        map.forEach((p, i) -> sb.append("PREFIX ").append(p).append(": ").append("<").append(i).append(">\n"));
        sb.append("SELECT * {\n");
        for (int j = 0; j < 10; ++j) {
            sb.append("  ");
            for (int i = 0; i < 3; ++i) {
                sb.append(gen.apply("node_" + j + "_" + i));
            }
            sb.append(".\n");
        }
        sb.append("}\n");
        String queryStr = sb.toString();
        return queryStr;
    }

    public static Function<String, String> randomCurieSupplier(Random rand, List<String> prefixes, Map<String, String> pmap) {
        return localName -> {
            int i = rand.nextInt(prefixes.size());
            String prefix = prefixes.get(i);
            String ns = pmap.get(prefix);
            return "<" + ns + localName + ">";
        };
    }


    public static void mainQueryParse(String[] args) {
    }

    public static void iterate(Supplier<PrefixMap> pmFactory) {

        PrefixMap pm = pmFactory.get();


        Prologue prologue = new Prologue(new PrefixMappingAdapter(pm));
        Query query = new Query(prologue);





    }

    public static void mainAbbrev(String[] args) throws Exception {

        String[][] baseIris = new String[2][];
        baseIris[0] = new String[]{"http://example.org/", "/"};
        baseIris[1]  = new String[]{ "urn:foo:bar:", ":"};

        for (int runId = 0; runId < 5; ++ runId) {
            for (int baseId = 0; baseId < baseIris.length; ++baseId) {
                String[] e = baseIris[baseId];
                String baseIriStr = e[0];
                String separator = e[1];

                for(int approachId = 0; approachId < 2; ++approachId) {
                    // Select the prefix map implementation: 0 -> improved, 1 -> original
                    PrefixMap pm = approachId == 0 ? new PrefixMapStdOrig() : new PrefixMapStd();

                    // Initialize some prefixes
                    for (int i = 0; i < 2000; ++i) {
                        pm.add("ns" + i, baseIriStr + i + separator);
                    }

                    // Lookup with the same IRI (always cache hit)
                    Stopwatch sw = Stopwatch.createStarted();
                    String staticIri = baseIriStr + "1" + separator + "foobar";
                    for (int i = 0; i < 1000000; ++i) {
                        String abbr = pm.abbreviate(staticIri);
                    }
                    System.out.println(String.format("Run %d with base <%s> and separator %s using approach %d: Static IRI lookups took %.3f seconds", runId, baseIriStr, separator, approachId, sw.elapsed(TimeUnit.MILLISECONDS) * 0.001));

                    // Lookup with different IRIs
                    Stopwatch sw2 = Stopwatch.createStarted();
                    for (int i = 0; i < 1000000; ++i) {
                        String iriStr = baseIriStr + (i % 10000) + separator + "foobar";
                        String abbr = pm.abbreviate(iriStr);
                    }
                    System.out.println(String.format("Run %d with base <%s> and separator %s using approach %d: Dynamic IRI lookups took %.3f seconds", runId, baseIriStr, separator, approachId, sw2.elapsed(TimeUnit.MILLISECONDS) * 0.001));

                    System.out.println();
                }
            }
        }
    }

}
