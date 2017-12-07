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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMRTreeSearchCursor extends LSMRTreeAbstractCursor {

    private int currentCursor;
    private final PermutingTupleReference btreeTuple;
    private boolean resultOfsearchCallBackProceed = false;
    private boolean useProceedResult = false;
    private byte[] valuesForOperationCallbackProceedReturnResult;
    private int numberOfFieldFromIndex = 0;
    private ArrayTupleBuilder tupleBuilderForProceedResult;
    private ArrayTupleReference copyTuple = null;

    public LSMRTreeSearchCursor(ILSMIndexOperationContext opCtx, int[] buddyBTreeFields) {
        super(opCtx);
        currentCursor = 0;
        this.btreeTuple = new PermutingTupleReference(buddyBTreeFields);
        this.useProceedResult = opCtx.getUseOpCallbackProceedResult();
        this.valuesForOperationCallbackProceedReturnResult = opCtx.getFirstValueForUseProceedResult();
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        currentCursor = 0;
    }

    @Override
    public void reset() throws HyracksDataException {
        if (!open) {
            return;
        }

        currentCursor = 0;
        foundNext = false;
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                rtreeCursors[i].close();
                btreeCursors[i].close();
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        ILSMComponentFilter filter = getFilter();
        return filter == null ? null : filter.getMinTuple();
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        ILSMComponentFilter filter = getFilter();
        return filter == null ? null : filter.getMaxTuple();
    }

    private ILSMComponentFilter getFilter() {
        return foundNext ? operationalComponents.get(currentCursor).getLSMComponentFilter() : null;
    }

    private void searchNextCursor() throws HyracksDataException {
        if (currentCursor < numberOfTrees) {
            rtreeCursors[currentCursor].reset();
            rtreeAccessors[currentCursor].search(rtreeCursors[currentCursor], rtreeSearchPredicate);
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (foundNext) {
            return true;
        }
        while (currentCursor < numberOfTrees) {
            while (rtreeCursors[currentCursor].hasNext()) {
                rtreeCursors[currentCursor].next();
                ITupleReference currentTuple = rtreeCursors[currentCursor].getTuple();
                // Call proceed() to do necessary operations before returning this tuple.
                resultOfsearchCallBackProceed = searchCallback.proceed(currentTuple);
                btreeTuple.reset(rtreeCursors[currentCursor].getTuple());
                boolean killerTupleFound = false;
                for (int i = 0; i < currentCursor && !killerTupleFound; i++) {
                    if (bloomFilters[i] != null && bloomFilters[i].contains(btreeTuple, hashes)) {
                        continue;
                    }
                    btreeCursors[i].reset();
                    btreeRangePredicate.setHighKey(btreeTuple, true);
                    btreeRangePredicate.setLowKey(btreeTuple, true);
                    btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                    try {
                        if (btreeCursors[i].hasNext()) {
                            killerTupleFound = true;
                        }
                    } finally {
                        btreeCursors[i].close();
                    }
                }
                if (!killerTupleFound) {
                    frameTuple = currentTuple;
                    foundNext = true;
                    return true;
                }
            }
            rtreeCursors[currentCursor].close();
            currentCursor++;
            searchNextCursor();
        }
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        foundNext = false;
        //  If useProceedResult is set to true and the result of searchCallback.proceed() is
        //     fail: zero (default value) will be added as a field.
        //  success: one (default value) will be added as a field.
        if (useProceedResult) {
            tupleBuilderForProceedResult.reset();
            TupleUtils.copyTuple(tupleBuilderForProceedResult, frameTuple, numberOfFieldFromIndex);

            if (!resultOfsearchCallBackProceed) {
                // fail case
                tupleBuilderForProceedResult.addField(valuesForOperationCallbackProceedReturnResult, 0, 5);
            } else {
                // success case
                tupleBuilderForProceedResult.addField(valuesForOperationCallbackProceedReturnResult, 5, 5);
            }
            copyTuple.reset(tupleBuilderForProceedResult.getFieldEndOffsets(),
                    tupleBuilderForProceedResult.getByteArray());
            frameTuple = copyTuple;
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        super.open(initialState, searchPred);
        searchNextCursor();
        // If it is required to use the result of searchCallback.proceed(),
        // we need to initialize the byte array that contains fail and success result.
        if (useProceedResult) {
            if (rtreeSearchPredicate.getLowKeyComparator() != null) {
                numberOfFieldFromIndex =
                        btreeCmp.getKeyFieldCount() + rtreeSearchPredicate.getLowKeyComparator().getKeyFieldCount();
            } else {
                throw HyracksDataException.create(ErrorCode.PREDICATE_CANNOT_BE_NULL);
            }
            tupleBuilderForProceedResult = new ArrayTupleBuilder(numberOfFieldFromIndex + 1);
            copyTuple = new ArrayTupleReference();
        }

    }

}
