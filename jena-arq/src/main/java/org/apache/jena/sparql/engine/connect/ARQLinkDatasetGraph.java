package org.apache.jena.sparql.engine.connect;

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.QueryExecBuilder;
import org.apache.jena.sparql.exec.QueryExecDatasetBuilder;
import org.apache.jena.sparql.exec.UpdateExecBuilder;
import org.apache.jena.sparql.exec.UpdateExecDatasetBuilder;

public class ARQLinkDatasetGraph
    implements ARQLink
{
    private final DatasetGraph dsg;

    public ARQLinkDatasetGraph(DatasetGraph dsg) {
        super();
        this.dsg = dsg;
    }

    @Override
    public QueryExecBuilder newQuery() {
        return QueryExecDatasetBuilder.create().dataset(dsg);
    }

    @Override
    public UpdateExecBuilder newUpdate() {
        return UpdateExecDatasetBuilder.create().dataset(dsg);
    }
}
