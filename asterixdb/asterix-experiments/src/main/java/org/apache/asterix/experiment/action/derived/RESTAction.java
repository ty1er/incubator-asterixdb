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

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;

import org.apache.asterix.experiment.action.base.AbstractAction;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

public abstract class RESTAction extends AbstractAction {

    private final String endpoint;
    private final String acceptHeaderType;
    private final String host;
    private final int port;
    protected EntityBuilder entityBuilder;
    private final CloseableHttpClient httpClient;
    private final String httpMethod;

    public RESTAction(String endpoint, String acceptHeaderType, String host, int port, CloseableHttpClient httpClient,
            String httpMethod) {
        super("REST");
        this.endpoint = endpoint;
        this.acceptHeaderType = acceptHeaderType;
        this.host = host;
        this.port = port;
        this.httpClient = httpClient;
        this.httpMethod = httpMethod;
        this.entityBuilder = EntityBuilder.create();
    }

    @Override
    protected void doPerform() throws Exception {
        String uri = MessageFormat.format(endpoint, host, String.valueOf(port));
        HttpEntity respEntity = null;
        CloseableHttpResponse resp = null;
        try {
            RequestBuilder requestBuilder =
                    RequestBuilder.create(httpMethod).setUri(uri).setHeader(HttpHeaders.ACCEPT, acceptHeaderType)
                            .setEntity(entityBuilder.build());
            if (entityBuilder.getParameters() != null && entityBuilder.getParameters().size() > 0)
                requestBuilder.addParameters(
                        entityBuilder.getParameters().toArray(new NameValuePair[entityBuilder.getParameters().size()]));

            resp = httpClient.execute(requestBuilder.build());
            respEntity = resp.getEntity();
            if (resp.getStatusLine().getStatusCode() != 200) {
                respEntity.writeTo(System.err);
                throw new Exception("REST call failed:" + respEntity.toString());
            }
            if (respEntity != null && respEntity.isStreaming()) {
                printStream(respEntity.getContent());
            }
        } finally {
            EntityUtils.consume(respEntity);
            if (resp != null) {
                resp.close();
            }
        }
    }

    protected abstract void printStream(InputStream content) throws IOException;
}
