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

import java.io.OutputStream;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.AbstractAction;
import org.apache.asterix.experiment.action.base.IAction;

public class TimedAction extends AbstractAction {

    private final Logger LOGGER = Logger.getLogger(TimedAction.class.getName());

    private final IAction action;
    private final OutputStream os;
    private final Function<Long, String> logMessage;

    public TimedAction(IAction action) {
        this(action, null, null);
    }

    public TimedAction(IAction action, Function<Long, String> logMessage) {
        this(action, null, logMessage);
    }

    public TimedAction(IAction action, OutputStream os) {
        this(action, os, null);
    }

    public TimedAction(IAction action, OutputStream os, Function<Long, String> logMessage) {
        super("timer");
        this.action = action;
        this.os = os;
        this.logMessage = logMessage;
    }

    @Override
    protected void doPerform() throws Exception {
        long start = System.nanoTime();
        action.perform();
        long end = System.nanoTime();
        long elapsedMs = (end - start) / 1000000;
        String output;
        if (logMessage != null) {
            output = logMessage.apply(elapsedMs);
        } else if (os == null) {
            output = "Elapsed time = " + elapsedMs + " for action " + action;
        } else
            output = elapsedMs + "\n";

        if (LOGGER.isLoggable(Level.INFO) && os == null) {
            LOGGER.info(output);
        } else {
            os.write(output.getBytes());
            os.flush();
        }
    }
}
