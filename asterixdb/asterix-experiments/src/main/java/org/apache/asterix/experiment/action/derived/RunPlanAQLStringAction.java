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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.ws.rs.HttpMethod;

import org.apache.asterix.common.utils.Servlets;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;

import com.google.common.collect.Lists;

public class RunPlanAQLStringAction extends RESTAction {
    private final Logger LOGGER = Logger.getLogger(RunPlanAQLStringAction.class.getName());
    protected static final String REST_URI_TEMPLATE = "http://{0}:{1}" + Servlets.QUERY_PLAN;
    private final OutputStream os;
    private final String query;

    public RunPlanAQLStringAction(CloseableHttpClient httpClient, String restHost, int restPort, String query,
            OutputStream os) {
        super(REST_URI_TEMPLATE, "application/json", restHost, restPort, httpClient, HttpMethod.POST);
        this.query = query;
        this.os = os;
    }

    @Override
    public void doPerform() throws Exception {
        entityBuilder.setParameters(Lists.newArrayList(new BasicNameValuePair("print-optimized-logical-plan", "true"),
                new BasicNameValuePair("query", query), new BasicNameValuePair("execute-query", "true")));
        super.doPerform();
    }

    @Override
    protected void printStream(InputStream content) throws IOException {
        OutputStream out = os;
        if (os == null) {
            out = System.out;
        }
        JsonReader reader = Json.createReader(content);
        Writer writer = new OutputStreamWriter(out);
        try {
            JsonObject obj = reader.readObject();
            if (obj.get("error-code") != null) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Error during query evaluation: code=" + obj.get("error-code") + " summary=" + obj
                            .get("summary") + " stacktrace" + obj.get("stacktrace"));
                }
            }
            boolean first = true;
            if (!first) {
                writer.write(",");
            }
            writer.write(obj.getJsonNumber("exec-time").longValue() + ",");
            writer.write(obj.getJsonNumber("optimize-time").longValue() + ",");
            writer.write(obj.getJsonNumber("estimate-time").longValue() + ",");
            //Extracting query results
            JsonArray results = obj.getJsonArray("result");
            for (JsonValue v : results) {
                if (v.getValueType().equals(JsonValue.ValueType.STRING))
                    writer.write(((JsonString) v).getString().trim());
            }
            //writing the estimated result cardinality
            writer.write(",");
            JsonArray plan = obj.getJsonArray("optimized-plan");
            //find select operator
            for (int i = 0; i < plan.size(); i++) {
                if (((JsonString) plan.getJsonObject(i).get("operator")).getString().equals("select")) {
                    writer.write(plan.getJsonObject(i).get("cardinality").toString());
                }
            }
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Optimized plan: " + plan);
            }
        } finally {
            writer.flush();
            reader.close();
        }
    }
}
