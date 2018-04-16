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

public class RunQueryStringAction extends RunQueryAction {

    private final String query;

    public RunQueryStringAction(CloseableHttpClient httpClient, String restHost, int restPort, String query) {
        this(httpClient, restHost, restPort, query, null);
    }

    public RunQueryStringAction(CloseableHttpClient httpClient, String restHost, int restPort, String query,
            OutputStream os) {
        super(httpClient, restHost, restPort, os);
        this.query = query;
    }

    public RunQueryStringAction(CloseableHttpClient httpClient, String restHost, int restPort, String query,
            OutputStream os, String contentType) {
        super(httpClient, restHost, restPort, os, contentType);
        this.query = query;
    }

    @Override
    public void doPerform() throws Exception {
        performQueryAction(query);
    }
}
