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
package org.apache.asterix.test.statistics;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.statistics.message.ReportFlushComponentStatisticsMessage;
import org.apache.asterix.statistics.message.ReportMergeComponentStatisticsMessage;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;

import com.google.common.base.Objects;

public class TestStatisticsMessageBroker implements INCMessageBroker {
    static class TestStatisticsMessageID {
        private String dataverse;
        private String dataset;
        private String index;
        private String field;
        private String node;
        private String partition;
        private boolean isAntimatter;

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestStatisticsMessageID that = (TestStatisticsMessageID) o;
            return Objects.equal(dataverse, that.dataverse) && Objects.equal(dataset, that.dataset)
                    && Objects.equal(index, that.index) && Objects.equal(field, that.field)
                    && Objects.equal(node, that.node) && Objects.equal(partition, that.partition)
                    && Objects.equal(isAntimatter, that.isAntimatter);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(dataverse, dataset, index, field, node, partition, isAntimatter);
        }

        @Override
        public String toString() {
            return "TestStatisticsMessageID{" + "dataverse='" + dataverse + '\'' + ", dataset='" + dataset + '\''
                    + ", index='" + index + '\'' + ", field='" + field + '\'' + ", node='" + node + '\''
                    + ", partition='" + partition + '\'' + ", isAntimatter=" + isAntimatter + '}';
        }

        public TestStatisticsMessageID(String dataverse, String dataset, String index, String field, String node,
                String partition, boolean isAntimatter) {
            this.dataverse = dataverse;
            this.dataset = dataset;
            this.index = index;
            this.field = field;
            this.node = node;
            this.partition = partition;
            this.isAntimatter = isAntimatter;
        }
    }

    class TestStatisticsMessageEntry implements Comparable<TestStatisticsMessageEntry> {

        private ComponentStatisticsId componentId;

        private ISynopsis<? extends ISynopsisElement<Long>> synopsis;

        public TestStatisticsMessageEntry(ComponentStatisticsId componentId,
                ISynopsis<? extends ISynopsisElement<Long>> synopsis) {
            this.componentId = componentId;
            this.synopsis = synopsis;
        }

        public ComponentStatisticsId getComponentId() {
            return componentId;
        }

        public ISynopsis<? extends ISynopsisElement<Long>> getSynopsis() {
            return synopsis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestStatisticsMessageEntry that = (TestStatisticsMessageEntry) o;
            return componentId.equals(that.componentId);
        }

        @Override
        public int hashCode() {
            return componentId.hashCode();
        }

        @Override
        public int compareTo(TestStatisticsMessageEntry o) {
            return componentIdComparator.compare(this.componentId, o.componentId);
        }
    }

    private static Comparator<ComponentStatisticsId> componentIdComparator = (o1, o2) -> {
        int startTimestampCompare = o1.getMinTimestamp().compareTo(o2.getMinTimestamp());
        if (startTimestampCompare != 0) {
            return startTimestampCompare;
        }
        return o1.getMaxTimestamp().compareTo(o2.getMaxTimestamp());
    };

    private Map<TestStatisticsMessageID, PriorityQueue<TestStatisticsMessageEntry>> statsMessages = new HashMap<>();

    public Map<TestStatisticsMessageID, PriorityQueue<TestStatisticsMessageEntry>> getStatsMessages() {
        return statsMessages;
    }

    @Override
    public void sendMessageToPrimaryCC(ICcAddressedMessage message) throws Exception {
        ReportFlushComponentStatisticsMessage statsMsg = (ReportFlushComponentStatisticsMessage) message;
        TestStatisticsMessageID newId =
                new TestStatisticsMessageID(statsMsg.getDataverse(), statsMsg.getDataset(), statsMsg.getIndex(),
                        statsMsg.getField(), statsMsg.getNode(), statsMsg.getPartition(), statsMsg.isAntimatter());
        PriorityQueue<TestStatisticsMessageEntry> entries =
                statsMessages.computeIfAbsent(newId, (k) -> new PriorityQueue<>());
        if (message instanceof ReportMergeComponentStatisticsMessage) {
            Iterator<TestStatisticsMessageEntry> eIt = entries.iterator();
            TestStatisticsMessageEntry currEntry = null;
            List<ComponentStatisticsId> mergedIds =
                    ((ReportMergeComponentStatisticsMessage) message).getMergeComponentIds();
            mergedIds.sort(componentIdComparator);
            for (ComponentStatisticsId deleteId : mergedIds) {
                while ((currEntry == null || componentIdComparator.compare(currEntry.componentId, deleteId) < 0)
                        && eIt.hasNext()) {
                    currEntry = eIt.next();
                }
                if (currEntry != null && currEntry.componentId.equals(deleteId)) {
                    eIt.remove();
                }
            }
        }
        entries.add(new TestStatisticsMessageEntry(statsMsg.getComponentId(), statsMsg.getSynopsis()));
    }

    @Override
    public void sendMessageToCC(CcId ccId, ICcAddressedMessage message) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void sendMessageToNC(String nodeId, INcAddressedMessage message) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void queueReceivedMessage(INcAddressedMessage msg) {
        throw new NotImplementedException();
    }

    @Override
    public MessageFuture registerMessageFuture() {
        return null;
    }

    @Override
    public MessageFuture deregisterMessageFuture(long futureId) {
        return null;
    }

    @Override
    public void receivedMessage(IMessage message, String nodeId) throws Exception {
        throw new NotImplementedException();
    }
}
