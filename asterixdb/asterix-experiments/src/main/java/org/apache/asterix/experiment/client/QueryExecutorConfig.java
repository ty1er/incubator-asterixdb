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

import org.apache.hyracks.http.server.utils.HttpUtil;
import org.kohsuke.args4j.Option;

public class QueryExecutorConfig {
    @Option(name = "-q", aliases = "--query", usage = "Query file", required = true)
    private String query;

    @Option(name = "-o", aliases = "--output", usage = "Output file", required = true)
    private String output;

    @Option(name = "-t", aliases = "--output-type", usage = "Type of the output file", required = true)
    private OutputType outputType;

    enum OutputType {
        CSV(HttpUtil.ContentType.CSV), ADM(HttpUtil.ContentType.APPLICATION_ADM), JSON(
                HttpUtil.ContentType.APPLICATION_JSON);

        private String contentType;

        OutputType(String contentType) {
            this.contentType = contentType;
        }

        public String getContentType() {
            return contentType;
        }
    }

    @Option(name = "-h", aliases = "--rest-host", usage = "REST API host")
    private String host = "localhost";

    @Option(name = "-p", aliases = "--rest-port", usage = "REST API port")
    private int port = 19002;

    public String getQuery() {
        return query;
    }

    public String getOutput() {
        return output;
    }

    public OutputType getOutputType() {
        return outputType;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
