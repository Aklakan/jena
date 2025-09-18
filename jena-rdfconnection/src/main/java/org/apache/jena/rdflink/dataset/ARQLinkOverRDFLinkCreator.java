package org.apache.jena.rdflink.dataset;

import org.apache.jena.atlas.lib.Creator;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.sparql.engine.connect.ARQLink;
import org.apache.jena.sparql.exec.QueryExecBuilder;
import org.apache.jena.sparql.exec.UpdateExecBuilder;

public class ARQLinkOverRDFLinkCreator
    implements ARQLink
{
    private Creator<RDFLink> rdfLinkCreator;

    public ARQLinkOverRDFLinkCreator(Creator<RDFLink> rdfLinkCreator) {
        super();
        this.rdfLinkCreator = rdfLinkCreator;
    }

    @Override
    public QueryExecBuilder newQuery() {
        // FIXME QueryExecBuilder must be deferred in order to properly close the link.
        return rdfLinkCreator.create().newQuery();
    }

    @Override
    public UpdateExecBuilder newUpdate() {
        // FIXME QueryExecBuilder must be deferred in order to properly close the link.
        return rdfLinkCreator.create().newUpdate();
    }
}
