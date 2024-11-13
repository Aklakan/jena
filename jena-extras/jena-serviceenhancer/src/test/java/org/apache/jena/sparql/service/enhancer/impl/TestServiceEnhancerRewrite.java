package org.apache.jena.sparql.service.enhancer.impl;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.service.enhancer.init.ServiceEnhancerInit;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

public class TestServiceEnhancerRewrite {
    // Ensure extensions are initialized
    static { JenaSystem.init(); }

    @Test
    public void test01() {
        ServiceEnhancerInit.init();

        Query nonScopeRestrictedQuery = QueryFactory.create("""
            SELECT * {
              BIND("foo" AS ?foo)
              SERVICE <cache:loop+scoped:> {
                SELECT ?bar {
                  BIND(?foo AS ?bar)
                }
              }
            }
        """);

        Query scopeRestrictedQuery = QueryFactory.create("""
            SELECT * {
              BIND("foo" AS ?foo)
              SERVICE <cache:loop+scoped:> {
                SELECT ?foo ?bar {
                  BIND(?foo AS ?bar)
                }
              }
            }
        """);

        Binding b1 = MoreQueryExecUtils.evalToBinding(QueryExec.graph(GraphFactory.createDefaultGraph()).query(nonScopeRestrictedQuery).build(), ServiceEnhancerInit::wrapOptimizer);
        Binding b2 = MoreQueryExecUtils.evalToBinding(QueryExec.graph(GraphFactory.createDefaultGraph()).query(scopeRestrictedQuery).build(), ServiceEnhancerInit::wrapOptimizer);

        System.out.println(b1);
        System.out.println(b2);

//        BatchQueryRewriter rewriter = BatchQueryRewriterBuilder.from(new OpServiceInfo(op), Var.alloc("idx"))
//                .setSequentialUnion(false)
//                .setOrderRetainingUnion(false)
//                .setOmitEndMarker(false)
//                .setSubstitutionStrategy(SubstitutionStrategy.SUBSTITUTE)
//                .build();

    }
}
