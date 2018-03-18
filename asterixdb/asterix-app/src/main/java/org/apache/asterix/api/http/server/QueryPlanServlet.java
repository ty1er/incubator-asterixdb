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
package org.apache.asterix.api.http.server;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryPlanServlet extends QueryApiServlet {
    private static final Logger LOGGER = Logger.getLogger(QueryPlanServlet.class.getName());

    public QueryPlanServlet(ConcurrentMap<String, Object> ctx, String[] paths, ICcApplicationContext appCtx,
            ILangCompilationProvider compilationProvider, IStatementExecutorFactory statementExecutorFactory,
            IStorageComponentProvider componentProvider) {
        super(ctx, paths, appCtx, compilationProvider, statementExecutorFactory, componentProvider);
    }

    private SessionOutput init(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        SessionConfig sessionConfig = new SessionConfig(SessionConfig.OutputFormat.CLEAN_JSON, true, true, true);
        sessionConfig.setOOBData(false, false, true, true, false);
        return new SessionOutput(sessionConfig, response.writer());
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        try {
            String query = getQueryParameter(request);
            SessionOutput sessionOutput = init(request, response);
            IStatementExecutor.Stats executionStats = new IStatementExecutor.Stats();
            doHandle(response, query, sessionOutput, QueryTranslator.ResultDelivery.IMMEDIATE, executionStats);
            sessionOutput.getJsonNode().put("exec-time", executionStats.getExecTime());
            sessionOutput.getJsonNode().put("optimize-time", executionStats.getOptimizationTime());
            sessionOutput.getJsonNode().put("estimate-time", executionStats.getEstimationTime());
            sessionOutput.writeJson();
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            LOGGER.log(Level.WARNING, "Failure handling request", e);
        }
    }
}
