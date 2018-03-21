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
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.statistics.message.ReportFlushComponentStatisticsMessage;
import org.apache.asterix.statistics.message.ReportMergeComponentStatisticsMessage;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;

public class StatisticsManager implements IStatisticsManager {

    class SynopsisEntry {
        private final ISynopsis synopsis;
        private final String dataverse;
        private final String dataset;
        private final String index;
        private final String field;

        SynopsisEntry(ISynopsis synopsis, String dataverse, String dataset, String index, String field) {
            this.synopsis = synopsis;
            this.dataverse = dataverse;
            this.dataset = dataset;
            this.index = index;
            this.field = field;
        }
    }

    private final INCServiceContext ncContext;
    //TODO:refactor this to use component IDs instead
    private MultiValuedMap<ILSMDiskComponent, SynopsisEntry> synopsisMap;
    private MultiValuedMap<ILSMDiskComponent, SynopsisEntry> antimatterSynopsisMap;

    public StatisticsManager(INCServiceContext ncApplicationContext) {
        ncContext = ncApplicationContext;
        synopsisMap = new HashSetValuedHashMap<>();
        antimatterSynopsisMap = new HashSetValuedHashMap<>();
    }

    private void sendMessage(ICcAddressedMessage msg) throws HyracksDataException {
        //TODO: make message sending routine asynchronous?
        try {
            ((INCMessageBroker) ncContext.getMessageBroker()).sendMessageToPrimaryCC(msg);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private List<String> parsePathComponents(String componentPath) throws HyracksDataException {
        //TODO: Find a more elegant way of getting dataverse/dataset/timestamp from stats rather then parsing filepaths
        String numPattern = "\\d";
        String namePattern = "([^" + File.separator + "]+)";
        String dirPattern = namePattern + File.separator;
        String indexDatasetPattern =
                namePattern + File.separator + numPattern + File.separator + namePattern + File.separator;
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

    private void sendFlushSynopsisStatistics(Collection<SynopsisEntry> flushComponentSynopses,
            ILSMDiskComponent newComponent, boolean isAntimatter) throws HyracksDataException {
        for (SynopsisEntry flushComponentSynopsis : flushComponentSynopses) {
            // send message only about non-empty statistics
            if (flushComponentSynopsis != null) {
                List<String> parsedComponentsPath =
                        parsePathComponents(((BTree) newComponent.getIndex()).getFileReference().getRelativePath());
                ICcAddressedMessage msg = new ReportFlushComponentStatisticsMessage(flushComponentSynopsis.synopsis,
                        flushComponentSynopsis.dataverse, flushComponentSynopsis.dataset, flushComponentSynopsis.index,
                        flushComponentSynopsis.field, ncContext.getNodeId(), parsedComponentsPath.get(1),
                        new ComponentStatisticsId(
                                LocalDateTime.parse(parsedComponentsPath.get(6), AbstractLSMIndexFileManager.FORMATTER),
                                LocalDateTime.parse(parsedComponentsPath.get(5),
                                        AbstractLSMIndexFileManager.FORMATTER)),
                        isAntimatter);
                sendMessage(msg);
            }
        }
    }

    private void sendMergeSynopsisStatistics(Collection<SynopsisEntry> flushComponentSynopses,
            ILSMDiskComponent newComponent, List<ILSMDiskComponent> mergedComponents, boolean isAntimatter)
            throws HyracksDataException {
        for (SynopsisEntry flushComponentSynopsis : flushComponentSynopses) {
            List<String> parsedComponentsPath =
                    parsePathComponents(((BTree) newComponent.getIndex()).getFileReference().getRelativePath());
            List<ComponentStatisticsId> mergedComponentIds = new ArrayList<>(mergedComponents.size());
            for (ILSMDiskComponent mergedComponent : mergedComponents) {
                List<String> parsedMergedComponentPath =
                        parsePathComponents(((BTree) mergedComponent.getIndex()).getFileReference().getRelativePath());
                mergedComponentIds.add(new ComponentStatisticsId(
                        LocalDateTime.parse(parsedMergedComponentPath.get(6), AbstractLSMIndexFileManager.FORMATTER),
                        LocalDateTime.parse(parsedMergedComponentPath.get(5), AbstractLSMIndexFileManager.FORMATTER)));
            }
            ICcAddressedMessage msg = new ReportMergeComponentStatisticsMessage(flushComponentSynopsis.synopsis,
                    flushComponentSynopsis.dataverse, flushComponentSynopsis.dataset, flushComponentSynopsis.index,
                    flushComponentSynopsis.field, ncContext.getNodeId(), parsedComponentsPath.get(1),
                    new ComponentStatisticsId(
                            LocalDateTime.parse(parsedComponentsPath.get(6), AbstractLSMIndexFileManager.FORMATTER),
                            LocalDateTime.parse(parsedComponentsPath.get(5), AbstractLSMIndexFileManager.FORMATTER)),
                    isAntimatter, mergedComponentIds);
            sendMessage(msg);
        }
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
    public void addStatistics(ISynopsis synopsis, String dataverse, String dataset, String index, String field,
            boolean isAntimatter, ILSMDiskComponent component) {
        SynopsisEntry newEntry = new SynopsisEntry(synopsis, dataverse, dataset, index, field);
        if (isAntimatter) {
            antimatterSynopsisMap.put(component, newEntry);
        } else {
            synopsisMap.put(component, newEntry);
        }
    }

}
