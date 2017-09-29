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
package org.apache.hyracks.storage.am.statistics.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

public class StatisticsMessageIOOperationCallback implements ILSMIOOperationCallback {
    private final ILSMIOOperationCallback wrapperIOOpCallback;
    private final IStatisticsManager statisticsMessageManager;

    public StatisticsMessageIOOperationCallback(ILSMIOOperationCallback wrapperIOOpCallback,
            IStatisticsManager statisticsMessageManager) {
        this.wrapperIOOpCallback = wrapperIOOpCallback;
        this.statisticsMessageManager = statisticsMessageManager;
    }

    @Override
    public void beforeOperation(LSMOperationType opType) throws HyracksDataException {
        wrapperIOOpCallback.beforeOperation(opType);
    }

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents,
            ILSMDiskComponent newComponent) throws HyracksDataException {
        wrapperIOOpCallback.afterOperation(opType, oldComponents, newComponent);
        if (opType == LSMOperationType.FLUSH) {
            statisticsMessageManager.sendFlushStatistics(newComponent);
        } else if (opType == LSMOperationType.MERGE) {
            List<ILSMDiskComponent> mergedConponentIDs = new ArrayList<>();
            for (ILSMComponent c : oldComponents) {
                mergedConponentIDs.add((ILSMDiskComponent) c);
            }
            statisticsMessageManager.sendMergeStatistics(newComponent, mergedConponentIDs);
        }
    }

    @Override
    public void afterFinalize(LSMOperationType opType, ILSMDiskComponent newComponent) throws HyracksDataException {
        wrapperIOOpCallback.afterFinalize(opType, newComponent);
    }

    @Override
    public void setNumOfMutableComponents(int count) {
        wrapperIOOpCallback.setNumOfMutableComponents(count);
    }
}
