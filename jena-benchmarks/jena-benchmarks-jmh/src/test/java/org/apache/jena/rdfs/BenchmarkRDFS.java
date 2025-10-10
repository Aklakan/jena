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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdfs.BenchmarkRDFSDataGenerator.Data;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSetOps;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Benchmark)
public class BenchmarkRDFS {

    // Set to true to start the main method for an IDE (without JMH).
    private static boolean debug = false;

    @Param({
        "4",
    })
    public int p1_classHierarchyDepth;

    @Param({
        "2",
    })
    public int p2_subClassesPerClass;

    @Param({
        "20000",
    })
    public int p3_instancesPerLeafClass;

    @Param({
        "5",
    })
    public int p4_subPropertyChainLength;

    @Param({
        "ANY_ANY_ANY",
        "ANY_ANY_Type",
        "ANY_a_Type",
        "ANY_P_ANY"
    })
    public String p5_query;

    private Data data;

    private Graph testGraph;
    private Query query;

    private long lastSeenResultCount = -1;

    @Param({
        "original",
        "reduced"
    })
    public String p6_rdfsImpl;

    @Benchmark
    public void run() throws Exception {
        boolean deduplicate = false;

        long count;
        try (QueryExec qe = QueryExec.graph(testGraph).query(query).build()) {
            if (!deduplicate) {
                count = RowSetOps.count(qe.select());
            } else {
                count = Iter.count(Iter.distinct(qe.select()));
            }

            if (true) {
                System.out.println("Counted: " + count);
            }
        }

        if (lastSeenResultCount != -1) {
            if (count != lastSeenResultCount) {
                throw new IllegalStateException("Benchmark result differed between iterations. Before: " + lastSeenResultCount + " Now: " + count);
            }
        }
        lastSeenResultCount = count;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        lastSeenResultCount = -1;

        if (debug) {
            p1_classHierarchyDepth = 3;
            p2_subClassesPerClass = 2;
            p3_instancesPerLeafClass = 3;
            p4_subPropertyChainLength = 1;
            p5_query = "ANY_a_Type";
            p6_rdfsImpl = "reduced";
        }

        data = BenchmarkRDFSDataGenerator.create()
            .setMaxClassHierarchyDepth(p1_classHierarchyDepth)
            .setChildrenPerClass(p2_subClassesPerClass)
            .setNumInstancesPerLeafClass(p3_instancesPerLeafClass)
            .setSubPropertyChainLength(p4_subPropertyChainLength)
            .generate();

        String queryStr = switch (p5_query) {
        case "ANY_ANY_ANY" -> "SELECT * { ?s ?p ?o }";
        case "ANY_ANY_Type" -> "SELECT * { ?s ?p <" + data.rootClass().getURI() + ">  }";
        case "ANY_a_Type" -> "SELECT * { ?s a <" + data.rootClass().getURI() + ">  }";
        case "ANY_P_ANY" -> "SELECT * { ?s <" + data.rootProperty().getURI() + "> ?o }";
        default -> throw new NoSuchElementException("Query id not mapped: " + p5_query);
        };

        query = QueryFactory.create(queryStr);

        SetupRDFS setup = RDFSFactory.setupRDFS(data.schema().getGraph());
        switch (p6_rdfsImpl) {
        case "original":
            testGraph = new GraphRDFS(data.data().getGraph(), setup);
            break;
        case "reduced":
            testGraph = new GraphRDFSReduced(data.data().getGraph(), setup);
            break;
        default:
            throw new NoSuchElementException("Unsupported implementation id: " + p5_query);
        }
    }

    public static ChainedOptionsBuilder getDefaults(Class<?> c) {
        return new OptionsBuilder()
                // Specify which benchmarks to run.
                // You can be more specific if you'd like to run only one benchmark per test.
                .include(c.getName())
                // Set the following options as needed
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.NONE)
                .warmupIterations(5)
                .measurementIterations(5)
                .measurementTime(TimeValue.NONE)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                //.jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
                .jvmArgs("-Xmx8G")
                //.addProfiler(WinPerfAsmProfiler.class)
                .resultFormat(ResultFormatType.JSON)
                .result(c.getSimpleName() + "_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + ".json");
    }

    public static void main(String[] args) throws Exception {
        if (debug) {
            BenchmarkRDFS benchmark = new BenchmarkRDFS();
            benchmark.setupTrial();
            benchmark.run();
        } else {
            Options opt = getDefaults(BenchmarkRDFS.class).build();
            new Runner(opt).run();
        }
    }
}
