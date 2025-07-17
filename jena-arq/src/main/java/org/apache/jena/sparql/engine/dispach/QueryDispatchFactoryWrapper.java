package org.apache.jena.sparql.engine.dispach;

import org.apache.jena.query.Query;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphWrapper;
import org.apache.jena.sparql.core.DatasetGraphWrapperView;
import org.apache.jena.sparql.engine.QueryEngineFactoryWrapper;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.util.Context;

/**
 * Default processing for a DatasetGraphWrapper - unwrap and repeat.
 * @see QueryEngineFactoryWrapper
 */
//public class QueryDispatchFactoryWrapper implements QueryDispatcher
//{
//    private static QueryDispatcher instance = new QueryDispatchFactoryWrapper() ;
//    public static QueryDispatcher get() { return instance ; }
//
//    @Override
//    public boolean accept(String queryString, Syntax syntax, DatasetGraph dsg, Context context) {
//        if ( !( dsg instanceof DatasetGraphWrapper ) || dsg instanceof DatasetGraphWrapperView )
//            return false ;
//        DatasetGraph dsg2 = ((DatasetGraphWrapper)dsg).getWrapped() ;
//        return QueryDispatcherRegistry.findFactory(queryString, syntax, dsg2, context).accept(queryString, syntax, dsg2, context) ;
//    }
//
//    @Override
//    public QueryExec create(String queryString, Syntax syntax, DatasetGraph dsg, Binding inputBinding, Context context) {
//        if ( !( dsg instanceof DatasetGraphWrapper ) || dsg instanceof DatasetGraphWrapperView )
//            return null ;
//        DatasetGraph dsg2 = ((DatasetGraphWrapper)dsg).getWrapped() ;
//        return QueryDispatcherRegistry.findFactory(queryString, syntax, dsg2, context).create(queryString, syntax, dsg2, inputBinding, context) ;
//    }
//
//    @Override
//    public boolean accept(Query query, DatasetGraph dsg, Context context) {
//        // DatasetGraphFilteredView changes the seen contents so we can't unwrap it for query.
//        if ( !( dsg instanceof DatasetGraphWrapper dsgw) || dsg instanceof DatasetGraphWrapperView )
//            return false ;
//        DatasetGraph dsg2 = dsgw.getWrapped() ;
//        return QueryEngineRegistry.findFactory(query, dsg2, context).accept(query, dsg2, context) ;
//    }
//
//    @Override
//    public QueryExec create(Query query, DatasetGraph dsg, Binding inputBinding, Context context) {
//        if ( !( dsg instanceof DatasetGraphWrapper dsgw) || dsg instanceof DatasetGraphWrapperView )
//            return null ;
//        DatasetGraph dsg2 = dsgw.getWrapped() ;
//        return QueryDispatcherRegistry.findFactory(query, dsg2, context).create(query, dsg2, inputBinding, context) ;
//    }
//}
