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

package org.apache.jena.sparql.exec.tracker;

import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.util.Context;

/** Track Update executions if an execution tracker is registered in their context. */
public class UpdateExecTransformExecTracking
    implements UpdateExecTransform
{
    private static final UpdateExecTransform INSTANCE = new UpdateExecTransformExecTracking();

    public static UpdateExecTransform get() {
        return INSTANCE;
    }

    @Override
    public UpdateExec transform(UpdateExec UpdateExec) {
        Context cxt = UpdateExec.getContext();
        // Returns a wrapper if tracking is applicable, otherwise returns the UpdateExec as-is.
        UpdateExec result = TaskEventBroker.track(cxt, UpdateExec);
        return result;
    }
}
