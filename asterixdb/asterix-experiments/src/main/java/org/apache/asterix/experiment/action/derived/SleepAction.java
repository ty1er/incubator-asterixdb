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

package org.apache.asterix.experiment.action.derived;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.AbstractAction;

public class SleepAction extends AbstractAction {
    private static final Logger LOGGER = Logger.getLogger(SleepAction.class.getName());

    private final long ms;

    public SleepAction(long ms) {
        super("sleep");
        this.ms = ms;
    }

    @Override
    protected void doPerform() throws Exception {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Sleeping for " + ms + "ms");
        }
        Thread.sleep(ms);
    }

}
