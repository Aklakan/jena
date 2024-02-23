package org.apache.jena.sparql.service.enhancer.impl;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.junit.Test;

public class TestServiceEnhancerQueryAbort {
    // @Test
    public void test_01() {
        LogCtl.setLogging();

        ExecutorService executorService = Executors.newCachedThreadPool();
        try {

            Model model = AbstractTestServiceEnhancerResultSetLimits.createModel(1000);

            // Produce a sufficiently large result set so that abort will surely hit in mid-execution
            Query query = QueryFactory.create("SELECT * { SERVICE <cache:> { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i . ?j ?k ?l } }");

            Random random = new Random();

            List<Integer> list = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
            list.parallelStream()
                    .forEach(i -> {
                        QueryExecution qe = QueryExecutionFactory.create(query, model);
                        Future<Integer> future = executorService.submit(() -> doCount(qe));
                        int delayToAbort = random.nextInt(100);
                        try {
                            Thread.sleep(delayToAbort);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        qe.abort();
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });


        } finally {
            executorService.shutdownNow();
        }
    }

    private static final int doCount(QueryExecution qe) {
        try (QueryExecution qe2 = qe) {
            ResultSet rs = qe2.execSelect();
            int size = ResultSetFormatter.consume(rs);
            return size;
        }
    }
}
