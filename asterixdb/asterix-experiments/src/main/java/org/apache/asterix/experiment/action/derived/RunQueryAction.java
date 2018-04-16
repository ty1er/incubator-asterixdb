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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.HttpMethod;

import org.apache.asterix.api.http.server.QueryServiceServlet.Parameter;
import org.apache.asterix.common.utils.Servlets;
import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class RunQueryAction extends RESTAction {
    private static final Logger LOGGER = Logger.getLogger(RunQueryAction.class.getName());

    protected final OutputStream os;

    public RunQueryAction(CloseableHttpClient httpClient, String restHost, int restPort, OutputStream os) {
        this(httpClient, restHost, restPort, os, HttpUtil.ContentType.APPLICATION_ADM);
    }

    public RunQueryAction(CloseableHttpClient httpClient, String restHost, int restPort, OutputStream os,
            String contentType) {
        super(contentType, restHost, restPort, httpClient, HttpMethod.POST);
        this.os = os;
    }

    @Override
    public String getEndpoint() {
        return REST_URI_TEMPLATE + Servlets.QUERY_SERVICE;
    }

    public void performQueryAction(String query) throws Exception {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Executing Query:\n" + query);
        }
        entityBuilder.setParameters(new NameValuePair() {
            @Override
            public String getName() {
                return Parameter.STATEMENT.str();
            }

            @Override
            public String getValue() {
                return query;
            }
        });
        super.doPerform();
    }

    @Override
    protected void printStream(InputStream content) throws IOException {
        OutputStream out = os;
        if (os == null) {
            out = System.out;
        }

        final ObjectMapper objectMapper = new ObjectMapper();
        JsonParser jsonParser = objectMapper.getFactory().createParser(content);
        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            if ("results".equals(jsonParser.getCurrentName())) {
                JsonToken t = jsonParser.nextToken();
                if (t != JsonToken.START_ARRAY) {
                    throw new IOException("Expected to see the start of result array:" + jsonParser.getCurrentToken());
                }
                t = jsonParser.nextToken();
                while (t != JsonToken.END_ARRAY) {
                    IOUtils.write(t.asString(), out);
                    t = jsonParser.nextToken();
                }
                break;
            }
            if ("status".equals(jsonParser.getCurrentName()) && "fatal".equals(jsonParser.nextToken().asString())) {
                throw new IOException("Error in query response:");
            }
        }
        out.flush();
    }

    public static class NoNewLineFileOutputStream extends OutputStream {

        private OutputStream outerStream;

        public NoNewLineFileOutputStream(OutputStream outerStream) throws FileNotFoundException {
            this.outerStream = outerStream;
        }

        @Override
        public void write(int c) throws IOException {
            byte b = (byte) c;
            if (b != -1 && b != '\n' && b != '\r') {
                outerStream.write(b);
            }
        }
    }
}
