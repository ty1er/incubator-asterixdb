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
package org.apache.asterix.statistics.common;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.statistics.message.ReportFlushComponentStatisticsMessage;
import org.apache.asterix.statistics.message.ReportMergeComponentStatisticsMessage;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;

public class StatisticsManager implements IStatisticsManager {

    private final INCServiceContext ncContext;
    private Map<ILSMDiskComponent, ISynopsis> synopsisMap;
    private Map<ILSMDiskComponent, ISynopsis> antimatterSynopsisMap;

    public StatisticsManager(INCServiceContext ncApplicationContext) {
        ncContext = ncApplicationContext;
        synopsisMap = new HashMap<>();
        antimatterSynopsisMap = new HashMap<>();
    }

    private void sendMessage(ICcAddressedMessage msg) throws HyracksDataException {
        //TODO: make message sending routine asynchronous
        try {
            ((INCMessageBroker) ncContext.getMessageBroker()).sendMessageToCC(msg);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private List<String> parsePathComponents(String componentPath) throws HyracksDataException {
        String namePattern = "([^" + File.separatorChar + "]+)";
        String dirPattern = namePattern + "\\" + File.separatorChar;
        String indexDatasetPattern =
                namePattern + StoragePathUtil.DATASET_INDEX_NAME_SEPARATOR + namePattern + "\\" + File.separatorChar;
        // Disk component name format: T2_T1_s. T2 & T1 are the same for flush component.
        // For merged component T2 is the max timestamp of the latest component, T1 - min timestamp of the earliest.
        String timestampPattern = "(\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{3})";

        StringBuilder regexpStringBuilder = new StringBuilder();
        //non-greedy pattern for storage directory name
        regexpStringBuilder.append(dirPattern).append("+?");
        //partition name
        regexpStringBuilder.append(dirPattern);
        //dataverse name
        regexpStringBuilder.append(dirPattern);
        //dataset & index names
        regexpStringBuilder.append(indexDatasetPattern);
        //component name
        regexpStringBuilder.append(timestampPattern).append(AbstractLSMIndexFileManager.DELIMITER)
                .append(timestampPattern).append(AbstractLSMIndexFileManager.DELIMITER)
                .append(AbstractLSMIndexFileManager.BTREE_SUFFIX);

        Pattern p = Pattern.compile(regexpStringBuilder.toString());
        Matcher m = p.matcher(componentPath);
        if (!m.matches()) {
            throw new HyracksDataException("Cannot parse out component's path");
        }

        List<String> results = new ArrayList<>();
        for (int i = 1; i <= m.groupCount(); i++) {
            results.add(m.group(i));
        }
        return results;
    }

    private void sendFlushSynopsisStatistics(ISynopsis flushComponentSynopsis, ILSMDiskComponent newComponent,
            boolean isAntimatter) throws HyracksDataException {
        // send message only about non-empty statistics
        if (flushComponentSynopsis != null) {
            List<String> parsedComponentsPath = parsePathComponents(
                    ((BTree) newComponent.getIndex()).getFileReference().getRelativePath());
            ICcAddressedMessage msg = new ReportFlushComponentStatisticsMessage(flushComponentSynopsis,
                    parsedComponentsPath.get(2), parsedComponentsPath.get(3), parsedComponentsPath.get(4),
                    ncContext.getNodeId(), parsedComponentsPath.get(1),
                    new ComponentStatisticsId(
                            LocalDateTime.parse(parsedComponentsPath.get(6), AbstractLSMIndexFileManager.FORMATTER),
                            LocalDateTime.parse(parsedComponentsPath.get(5), AbstractLSMIndexFileManager.FORMATTER)),
                    isAntimatter);
            sendMessage(msg);
        }
    }

    private void sendMergeSynopsisStatistics(ISynopsis flushComponentSynopsis, ILSMDiskComponent newComponent,
            List<ILSMDiskComponent> mergedComponents, boolean isAntimatter) throws HyracksDataException {
        List<String> parsedComponentsPath = parsePathComponents(
                ((BTree) newComponent.getIndex()).getFileReference().getRelativePath());
        List<ComponentStatisticsId> mergedComponentIds = new ArrayList<>(mergedComponents.size());
        for (ILSMDiskComponent mergedComponent : mergedComponents) {
            List<String> parsedMergedComponentPath = parsePathComponents(
                    ((BTree) mergedComponent.getIndex()).getFileReference().getRelativePath());
            mergedComponentIds.add(new ComponentStatisticsId(
                    LocalDateTime.parse(parsedMergedComponentPath.get(6), AbstractLSMIndexFileManager.FORMATTER),
                    LocalDateTime.parse(parsedMergedComponentPath.get(5), AbstractLSMIndexFileManager.FORMATTER)));
        }
        ICcAddressedMessage msg = new ReportMergeComponentStatisticsMessage(flushComponentSynopsis,
                parsedComponentsPath.get(2), parsedComponentsPath.get(3), parsedComponentsPath.get(4),
                ncContext.getNodeId(), parsedComponentsPath.get(1),
                new ComponentStatisticsId(
                        LocalDateTime.parse(parsedComponentsPath.get(6), AbstractLSMIndexFileManager.FORMATTER),
                        LocalDateTime.parse(parsedComponentsPath.get(5), AbstractLSMIndexFileManager.FORMATTER)),
                isAntimatter, mergedComponentIds);
        sendMessage(msg);
    }

    @Override
    public void sendFlushStatistics(ILSMDiskComponent flushedComponent) throws HyracksDataException {
        sendFlushSynopsisStatistics(synopsisMap.remove(flushedComponent), flushedComponent, false);
        sendFlushSynopsisStatistics(antimatterSynopsisMap.remove(flushedComponent), flushedComponent, true);
    }

    @Override
    public void sendMergeStatistics(ILSMDiskComponent newComponent, List<ILSMDiskComponent> mergedComponents)
            throws HyracksDataException {
        sendMergeSynopsisStatistics(synopsisMap.remove(newComponent), newComponent, mergedComponents, false);
        sendMergeSynopsisStatistics(antimatterSynopsisMap.remove(newComponent), newComponent, mergedComponents, true);
    }

    @Override
    public void addStatistics(ISynopsis synopsis, boolean isAntimatter, ILSMDiskComponent component) {
        if (isAntimatter) {
            antimatterSynopsisMap.put(component, synopsis);
        } else {
            synopsisMap.put(component, synopsis);
        }
    }

}
