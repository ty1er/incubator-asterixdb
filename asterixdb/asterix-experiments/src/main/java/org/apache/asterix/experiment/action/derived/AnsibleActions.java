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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;

import org.apache.asterix.experiment.action.base.SequentialActionList;

public class AnsibleActions {

    private enum AnsibleCommand {
        DEPLOY("deploy.sh"),
        START("start.sh"),
        STOP("stop.sh"),
        ERASE("erase.sh");

        private final String command;

        private AnsibleCommand(String command) {
            this.command = command;
        }

        public String getCommand() {
            return command;
        }
    }

    private static abstract class AbstractAnsibleCommandAction extends AbstractLocalExecutableAction {

        protected final String ansibleBinPath;
        protected final String ansibleConfPath;

        private AnsibleCommand command;

        protected AbstractAnsibleCommandAction(String path, String name) {
            super("ansible:" + name);
            Path ansiblePath = Paths.get(path);
            this.ansibleBinPath = ansiblePath.resolve("bin").toString();
            this.ansibleConfPath = ansiblePath.resolve("conf").toString();
        }

        protected AbstractAnsibleCommandAction(String path, AnsibleCommand command) {
            this(path, command.name());
            this.command = command;
        }

        @Override
        protected String getCommand() {
            return MessageFormat.format("{0}/{1}", ansibleBinPath, command.getCommand());
        }

        //        @Override
        //        protected Map<String, String> getEnvironment() {
        //            Map<String, String> env = new HashMap<>();
        //            env.put("MANAGIX_HOME", ansibleBinPath);
        //            return env;
        //        }

    }

    static class DeployAsterixAnsibleAction extends AbstractAnsibleCommandAction {
        public DeployAsterixAnsibleAction(String ansiblePath) {
            super(ansiblePath, AnsibleCommand.DEPLOY);
        }
    }

    public static class DeployAsterixAction extends SequentialActionList {
        public DeployAsterixAction(String ansiblePath, String clusterConfigPath, String inventoryFilePath) {
            super("deploy");
            addLast(new AbstractAnsibleCommandAction(ansiblePath, "copy cc.conf") {
                @Override
                protected String getCommand() {
                    return MessageFormat.format("cp {0} {1}/cc.conf", clusterConfigPath, ansibleConfPath);
                }
            });
            addLast(new AbstractAnsibleCommandAction(ansiblePath, "copy inventory") {
                @Override
                protected String getCommand() {
                    return MessageFormat.format("cp {0} {1}/inventory", inventoryFilePath, ansibleConfPath);
                }
            });
            addLast(new DeployAsterixAnsibleAction(ansiblePath));
        }
    }

    public static class StartAsterixAnsibleAction extends AbstractAnsibleCommandAction {

        public StartAsterixAnsibleAction(String ansiblePath) {
            super(ansiblePath, AnsibleCommand.START);
        }

    }

    public static class EraseAsterixAnsibleAction extends AbstractAnsibleCommandAction {

        public EraseAsterixAnsibleAction(String ansiblePath) {
            super(ansiblePath, AnsibleCommand.ERASE);
        }

    }

    public static class StopAsterixAnsibleAction extends AbstractAnsibleCommandAction {

        public StopAsterixAnsibleAction(String ansiblePath) {
            super(ansiblePath, AnsibleCommand.STOP);
        }

    }
}
