/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.experiment.action.base;

public abstract class AbstractAction implements IAction {

    protected final String name;
    private final IExceptionListener el;

    protected AbstractAction() {
        this(new DefaultExceptionListener(), null);
    }

    protected AbstractAction(String name) {
        this(new DefaultExceptionListener(), name);
    }

    protected AbstractAction(IExceptionListener el, String name) {
        this.el = el;
        this.name = name;
    }

    @Override
    public void perform() {
        try {
            doPerform();
        } catch (Throwable t) {
            el.caughtException(t);
        }
    }

    protected abstract void doPerform() throws Exception;

}
