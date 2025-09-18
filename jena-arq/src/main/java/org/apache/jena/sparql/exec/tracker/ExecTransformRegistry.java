package org.apache.jena.sparql.exec.tracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.dispatch.QueryDispatcher;
import org.apache.jena.sparql.engine.dispatch.QueryDispatcherOverRegistry;
import org.apache.jena.sparql.engine.dispatch.UpdateDispatcher;
import org.apache.jena.sparql.engine.dispatch.UpdateDispatcherOverRegistry;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.QueryExecTransformMain;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.exec.UpdateExecTransformMain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.update.UpdateRequest;

/**
 * The ExecTransformRegistry provides a plugin system for
 * how to execute SPARQL statements against DatasetGraphs.
 */
public class ExecTransformRegistry
{
    List<QueryExecTransform> queryExecTransforms = Collections.synchronizedList(new ArrayList<>());
    List<UpdateExecTransform> updateExecTransforms = Collections.synchronizedList(new ArrayList<>());

    // Singleton
    private static ExecTransformRegistry registry;
    static { init(); }

    static public ExecTransformRegistry get()
    {
        return registry;
    }

    public List<QueryExecTransform> getQueryDispatchers() {
        return queryExecTransforms;
    }

    public List<UpdateExecTransform> getUpdateDispatchers() {
        return updateExecTransforms;
    }

    /** If there is a registry in the context then return it otherwise yield the global instance */
    static public ExecTransformRegistry chooseRegistry(Context context)
    {
        ExecTransformRegistry result = get(context);
        if (result == null) {
            result = get();
        }
        return result;
    }

    /** Get the query engine registry from the context or null if there is none.
     *  Returns null if the context is null. */
    static public ExecTransformRegistry get(Context context)
    {
        ExecTransformRegistry result = context == null
                ? null
                : context.get(ARQConstants.registrySparqlDispatchers);
        return result;
    }

    static public void set(Context context, ExecTransformRegistry registry)
    {
        context.set(ARQConstants.registrySparqlDispatchers, registry);
    }

    public ExecTransformRegistry copy() {
        ExecTransformRegistry result = new ExecTransformRegistry();
        result.queryExecTransforms.addAll(queryExecTransforms);
        result.updateExecTransforms.addAll(updateExecTransforms);
        return result;
    }

    /** Create a copy of the registry from the context or return a new instance */
    public static ExecTransformRegistry copyFrom(Context context) {
        ExecTransformRegistry tmp = get(context);
        ExecTransformRegistry result = tmp != null
                ? tmp.copy()
                : new ExecTransformRegistry();

        return result;
    }

    public ExecTransformRegistry() { }

    private static void init()
    {
        registry = new ExecTransformRegistry();

//        registry.add(new QueryExecTransformMain());
//        registry.add(new UpdateExecTransformMain());
    }

    // ----- Query -----

    /** Add a query dispatcher to the default registry */
    public static void addDispatcher(QueryExecTransform f) { get().add(f); }

    /** Add a query dispatcher */
    public void add(QueryExecTransform f)
    {
        // Add to low end so that newer factories are tried first
        queryExecTransforms.add(0, f);
    }

    /** Remove a query dispatcher */
    public static void removeDispatcher(QueryExecTransform f)  { get().remove(f); }

    /** Remove a query dispatcher */
    public void remove(QueryExecTransform f)  { queryExecTransforms.remove(f); }

    /** Check whether a query dispatcher is already registered in the default registry */
    public static boolean containsFactory(QueryExecTransform f) { return get().contains(f); }

    /** Check whether a query dispatcher is already registered */
    public boolean contains(QueryExecTransform f) { return queryExecTransforms.contains(f); }

    public static QueryExec exec(Query query, DatasetGraph dsg, Binding initialBinding, Context context) {
        ExecTransformRegistry registry = chooseRegistry(context);
        QueryDispatcher queryDispatcher = new QueryDispatcherOverRegistry(registry);
        QueryExec qExec = queryDispatcher.create(query, dsg, initialBinding, context);
        return qExec;
    }

    // ----- Update -----

    /** Add an update dispatcher to the default registry */
    public static void addDispatcher(UpdateExecTransform f) { get().add(f); }

    /** Add an update dispatcher */
    public void add(UpdateExecTransform f)
    {
        // Add to low end so that newer factories are tried first
        updateExecTransforms.add(0, f);
    }

    /** Remove an update dispatcher */
    public static void removeDispatcher(UpdateExecTransform f)  { get().remove(f); }

    /** Remove an update dispatcher */
    public void remove(UpdateExecTransform f)  { updateExecTransforms.remove(f); }

    /** Check whether an update dispatcher is already registered in the default registry */
    public static boolean containsDispatcher(UpdateExecTransform f) { return get().contains(f); }

    /** Check whether an update dispatcher is already registered */
    public boolean contains(UpdateExecTransform f) { return updateExecTransforms.contains(f); }

    public static UpdateExec exec(UpdateRequest updateRequest, DatasetGraph dsg, Binding initialBinding, Context context) {
        ExecTransformRegistry registry = chooseRegistry(context);
        UpdateDispatcher updateDispatcher = new UpdateDispatcherOverRegistry(registry);
        UpdateExec uExec = updateDispatcher.create(updateRequest, dsg, initialBinding, context);
        return uExec;
    }

    // ----- Parse Check -----

//    public static void setParseCheck(Context cxt, Boolean value) {
//        cxt.set(ARQConstants.parseCheck, value);
//    }
//
//    public static Optional<Boolean> getParseCheck(DatasetGraph dsg) {
//        return Optional.ofNullable(dsg).map(DatasetGraph::getContext).flatMap(ExecTransformRegistry::getParseCheck);
//    }
//
//    public static Optional<Boolean> getParseCheck(Context cxt) {
//        return Optional.ofNullable(cxt).map(c -> c.get(ARQConstants.parseCheck));
//    }
//
//    public static Optional<Boolean> getParseCheck(ContextAccumulator cxtAcc) {
//        return Optional.ofNullable(cxtAcc).map(ca -> ca.get(ARQConstants.parseCheck));
//    }
//
//    public static boolean effectiveParseCheck(Optional<Boolean> parseCheck, Context cxt) {
//        return parseCheck.orElseGet(() -> getParseCheck(cxt).orElse(true));
//    }
//
//    public static boolean effectiveParseCheck(Optional<Boolean> parseCheck, ContextAccumulator cxtAcc) {
//        return parseCheck.orElseGet(() -> getParseCheck(cxtAcc).orElse(true));
//    }
}
