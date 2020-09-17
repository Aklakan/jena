/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.List;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <ComponentType>
 */
public class TupleConstraintImpl<ComponentType>
    implements TupleConstraint<ComponentType>
{
    protected List<ComponentType> constraints;

    public TupleConstraintImpl(List<ComponentType> constraints) {
        super();
        this.constraints = constraints;
    }

    public List<ComponentType> getConstraints() {
        return constraints;
    }
}
