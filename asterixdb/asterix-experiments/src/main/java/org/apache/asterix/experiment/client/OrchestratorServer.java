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

package org.apache.asterix.experiment.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.IAction;

public class OrchestratorServer {

    private static final Logger LOGGER = Logger.getLogger(OrchestratorServer.class.getName());

    private final int port;

    private final int nDataGens;

    private final int nIntervals;
    private final IAction startAction;
    private final IAction stopAction;

    private final AtomicBoolean running;

    private final IAction[] batchActions;

    public OrchestratorServer(int port, int nDataGens, int nIntervals, IAction startAction, IAction[] batchActions,
            IAction stopAction) {
        this.port = port;
        this.nDataGens = nDataGens;
        this.nIntervals = nIntervals;
        this.startAction = startAction;
        this.stopAction = stopAction;
        running = new AtomicBoolean();
        this.batchActions = batchActions;
    }

    public synchronized void start() throws IOException, InterruptedException {
        final AtomicBoolean bound = new AtomicBoolean();
        running.set(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerSocket ss = new ServerSocket(port);
                    ss.setSoTimeout(0);
                    synchronized (bound) {
                        bound.set(true);
                        bound.notifyAll();
                    }
                    Socket[] conn = new Socket[nDataGens];
                    try {
                        for (int i = 0; i < nDataGens; i++) {
                            conn[i] = ss.accept();
                        }
                        long startTS = -1;
                        for (int i = 0; i < nDataGens; i++) {
                            receiveMsg(conn[i], OrchestratorDGProtocol.START);
                            startTS = startTS == -1 ? System.nanoTime() : startTS;
                        }
                        startAction.perform();
                        for (int n = 0; n < nIntervals; ++n) {

                            long endTS = -1;
                            for (int i = 0; i < nDataGens; i++) {
                                receiveMsg(conn[i], OrchestratorDGProtocol.REACHED);
                                endTS = System.nanoTime();
                            }
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info(
                                        "Generating batch#" + (n + 1) + " took " + (endTS - startTS) / 1000000 + " ms");
                            }
                            startTS = endTS;
                            batchActions[n].perform();
                            if (n != nIntervals - 1) {
                                for (int i = 0; i < nDataGens; i++) {
                                    sendResume(conn[i]);
                                }
                            }
                        }
                    } catch (Throwable t) {
                        LOGGER.log(Level.SEVERE, "Exception in Orchestrator loop", t);
                    } finally {
                        for (int i = 0; i < nDataGens; i++) {
                            receiveMsg(conn[i], OrchestratorDGProtocol.STOPPED);
                        }
                        stopAction.perform();
                        for (int i = 0; i < conn.length; ++i) {
                            if (conn[i] != null) {
                                conn[i].close();
                            }
                        }
                        ss.close();
                    }
                    running.set(false);
                    synchronized (OrchestratorServer.this) {
                        OrchestratorServer.this.notifyAll();
                    }
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "Exception in Orchestrator loop", t);
                }
            }

        });
        t.start();
        synchronized (bound) {
            while (!bound.get()) {
                bound.wait();
            }
        }
    }

    private void sendResume(Socket conn) throws IOException {
        new DataOutputStream(conn.getOutputStream()).writeInt(OrchestratorDGProtocol.RESUME.ordinal());
        conn.getOutputStream().flush();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Sent " + OrchestratorDGProtocol.RESUME + " to " + conn.getRemoteSocketAddress());
        }
    }

    private void receiveMsg(Socket conn, OrchestratorDGProtocol msgType) throws IOException {
        int msg = new DataInputStream(conn.getInputStream()).readInt();
        OrchestratorDGProtocol receivedType = OrchestratorDGProtocol.values()[msg];
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Received " + msgType + " from " + conn.getRemoteSocketAddress());
        }
        if (receivedType != msgType) {
            throw new IllegalStateException("Encountered unknown message type " + msgType);
        }
    }

    public synchronized void awaitFinished() throws InterruptedException {
        while (running.get()) {
            wait();
        }
    }

}
