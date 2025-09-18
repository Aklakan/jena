package org.apache.jena.sparql.exec;

import org.apache.jena.sparql.engine.connect.ARQLink;
import org.apache.jena.sparql.engine.connect.ARQLinkProviderRegistry;

/**
 * QueryExecBuilder that chooses the actual builder only when build is called.
 */
public class QueryExecBuilderDeferred<X extends QueryExecBuilderDeferred<X>> extends QueryExecDatasetBuilderBase<X>
{
    public static QueryExecBuilder create() {
        return new QueryExecBuilderDeferred<>();
    }

    @Override
    public QueryExec build() {
        ARQLink link = ARQLinkProviderRegistry.connect(dataset);
        QueryExecBuilder qeb = link.newQuery();

        if (true) {
            throw new UnsupportedOperationException("passing on settings not yet implemented");
        }

        QueryExec qe = qeb.build();
        return qe;
    }
}
