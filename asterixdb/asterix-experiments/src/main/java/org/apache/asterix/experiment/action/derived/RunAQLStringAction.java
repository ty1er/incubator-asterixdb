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

import org.apache.http.impl.client.CloseableHttpClient;

public class RunAQLStringAction extends RunAQLAction {

    private final String aql;

    public RunAQLStringAction(CloseableHttpClient httpClient, String restHost, int restPort, String aql) {
        this(httpClient, restHost, restPort, aql, null);
    }

    public RunAQLStringAction(CloseableHttpClient httpClient, String restHost, int restPort, String aql,
            OutputStream os) {
        super(httpClient, restHost, restPort, os);
        this.aql = aql;
    }

    public RunAQLStringAction(CloseableHttpClient httpClient, String restHost, int restPort, String aql,
            OutputStream os, String contentType) {
        super(httpClient, restHost, restPort, os, contentType);
        this.aql = aql;
    }

    @Override
    public void doPerform() throws Exception {
        performAqlAction(aql);
    }
}
