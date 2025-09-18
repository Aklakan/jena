package org.apache.jena.sparql.exec.tracker;

import org.apache.jena.sparql.exec.UpdateExec;

public interface UpdateExecTransform {
    UpdateExec transform(UpdateExec updateExec);
}
