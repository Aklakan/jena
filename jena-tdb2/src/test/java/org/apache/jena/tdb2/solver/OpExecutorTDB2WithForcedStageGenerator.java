package org.apache.jena.tdb2.solver;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterDistinguishedVars;

public class OpExecutorTDB2WithForcedStageGenerator
    extends OpExecutorTDB2
{
    private final boolean hideBNodeVars;

    public OpExecutorTDB2WithForcedStageGenerator(ExecutionContext execCxt) {
        super(execCxt);
        this.hideBNodeVars = execCxt.getContext().isTrue(ARQ.hideNonDistiguishedVariables);

    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        BasicPattern pattern = opBGP.getPattern();
        QueryIterator qIter = stageGenerator.execute(pattern, input, execCxt);
        if ( hideBNodeVars )
            qIter = new QueryIterDistinguishedVars(qIter, execCxt);
        return qIter;
    }

    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        // Convert to BGP forms to execute in this graph-centric engine.
        if ( quadPattern.isDefaultGraph() && execCxt.getActiveGraph() == execCxt.getDataset().getDefaultGraph() ) {
            // Note we tested that the containing graph was the dataset's
            // default graph.
            // Easy case.
            OpBGP opBGP = new OpBGP(quadPattern.getBasicPattern());
            return execute(opBGP, input);
        }
        // Not default graph - (graph .... )
        OpBGP opBGP = new OpBGP(quadPattern.getBasicPattern());
        OpGraph op = new OpGraph(quadPattern.getGraphNode(), opBGP);
        return execute(op, input);
    }
}
