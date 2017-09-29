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
package org.apache.hyracks.storage.am.statistics.wavelet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.collections4.iterators.ReverseListIterator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;

public class WaveletSynopsis extends AbstractSynopsis<WaveletCoefficient> {

    private static final long serialVersionUID = 1L;

    // Trigger wavelet coefficients to be normalized
    private boolean normalize;
    // Trigger linear approximation of the prefix sum returned by synopsis
    private boolean linearApproximation;

    public WaveletSynopsis(long domainStart, long domainEnd, int maxLevel, int size,
            Collection<WaveletCoefficient> coefficients, boolean normalize, boolean linearApproximation) {
        super(domainStart, domainEnd, maxLevel, size, coefficients);
        this.normalize = normalize;
        this.linearApproximation = linearApproximation;
    }

    public boolean isNormalized() {
        return normalize;
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.Wavelet;
    }

    public List<WaveletCoefficient> sortOnKeys() {
        List<WaveletCoefficient> sortedCoefficients = new ArrayList<>(synopsisElements);
        Collections.sort(sortedCoefficients, WaveletCoefficient.KEY_COMPARATOR);
        return sortedCoefficients;
    }

    // Adds a new coefficient to the transform, subject to thresholding
    public void addElement(WaveletCoefficient coeff) {
        addElement(coeff.getKey(), coeff.getValue(), coeff.getLevel(), normalize);
    }

    // Adds a new detail coefficient to the transform, subject to thresholding
    public void addElement(long index, double value, int level, boolean normalize) {
        WaveletCoefficient detailCoeff;
        if (normalize)
            value /= WaveletCoefficient.getNormalizationCoefficient(maxLevel, level);
        if (synopsisElements.size() < size) {
            detailCoeff = new WaveletCoefficient(value, level, index);
        } else {
            detailCoeff = ((Queue<WaveletCoefficient>) synopsisElements).poll();
            if (Math.abs(value) > Math.abs(detailCoeff.getValue())) {
                detailCoeff.setValue(value);
                detailCoeff.setIndex(index);
                detailCoeff.setLevel(level);
            }
        }
        synopsisElements.add(detailCoeff);
    }

    class TreeNode {
        private TreeNode left;
        private TreeNode right;
        private WaveletCoefficient value;

        public TreeNode(WaveletCoefficient value) {
            this.value = value;
        }

        public TreeNode getLeft() {
            return left;
        }

        public TreeNode getRight() {
            return right;
        }

        public WaveletCoefficient getValue() {
            return value;
        }
    }

    //function sorts wavelet coefficients in binary tree preorder
    public void createBinaryPreorder() {
        // sorting coefficients according their indices first
        List<WaveletCoefficient> sortedCoefficients = sortOnKeys();
        synopsisElements = new ArrayList<>(synopsisElements.size());
        //scan through the queue to find main average
        TreeNode transformTreeRoot = null;
        boolean fakeRoot = false;
        for (WaveletCoefficient w : sortedCoefficients) {
            if (w.getKey() == 0) {
                synopsisElements.add(w);
                continue;
            } else if (w.getKey() == 1) {
                transformTreeRoot = new TreeNode(w);
                continue;
            } else if (transformTreeRoot == null) {
                //create fake main average for the sake of correctness of the algorithm
                transformTreeRoot = new TreeNode(new WaveletCoefficient(0.0, maxLevel, 1));
                fakeRoot = true;
                continue;
            }
            TreeNode insertedNode = new TreeNode(w);
            //traverse the tree root to leaf finding a place to insert the new node
            TreeNode activeNode = null;
            TreeNode nextNode = transformTreeRoot;
            boolean isLeft = false;
            while (nextNode != null) {
                if (nextNode.value.getLevel() < insertedNode.value.getLevel()) {
                    if (activeNode != null) {
                        if (isLeft) {
                            activeNode.left = insertedNode;
                        } else {
                            activeNode.right = insertedNode;
                        }
                    }
                    TreeNode tmp = nextNode;
                    nextNode = insertedNode;
                    insertedNode = tmp;
                } else {
                    //determine is it left or right child
                    //isLeft = (coeff.getKey() >> (coeff.getLevel() - nextNode.value.getLevel()) & 0x1) == 0;
                    activeNode = nextNode;
                    //determine is it left or right child
                    isLeft = (insertedNode.value.getKey() >> (Math.abs(insertedNode.value.getLevel() - nextNode.value
                            .getLevel()) - 1) & 0x1) == 0;
                    if (isLeft) {
                        nextNode = nextNode.left;
                    } else {
                        nextNode = nextNode.right;
                    }
                }
            }
            if (isLeft)
                activeNode.left = insertedNode;
            else
                activeNode.right = insertedNode;
        }

        preOrderTraversal(transformTreeRoot, transformTreeRoot, fakeRoot);
    }

    private void preOrderTraversal(TreeNode treeRoot, TreeNode node, boolean fakeRoot) {
        if (node != null && treeRoot != null) {
            if (!node.equals(treeRoot) || !fakeRoot)
                synopsisElements.add(node.getValue());
            if (node.getLeft() != null)
                preOrderTraversal(treeRoot, node.getLeft(), fakeRoot);
            if (node.getRight() != null)
                preOrderTraversal(treeRoot, node.getRight(), fakeRoot);
        }
    }

    @Override
    public boolean isMergeable() {
        return true;
    }

    @Override
    public void merge(List<ISynopsis<WaveletCoefficient>> synopsisList) throws HyracksDataException {
        super.merge(synopsisList);
        createBinaryPreorder();
    }

    @Override
    // Method implements naive synopsis merge, which just picks largest coefficients from the synopsis sum
    public void merge(ISynopsis<WaveletCoefficient> mergedSynopsis) throws HyracksDataException {
        if (mergedSynopsis.getType() != SynopsisType.Wavelet) {
            return;
        }
        // sort synopsis coefficients based on keys
        Iterator<WaveletCoefficient> mergedIt = ((WaveletSynopsis) mergedSynopsis).sortOnKeys().iterator();
        Iterator<WaveletCoefficient> it = sortOnKeys().iterator();
        synopsisElements.clear();
        synopsisElements = new PriorityQueue<>(size, WaveletCoefficient.VALUE_COMPARATOR);
        WaveletCoefficient mergedEntry = null;
        WaveletCoefficient entry = null;
        if (mergedIt.hasNext()) {
            mergedEntry = mergedIt.next();
        }
        if (it.hasNext()) {
            entry = it.next();
        }
        while (mergedEntry != null || entry != null) {
            if ((mergedEntry != null && entry == null)
                    || (mergedEntry != null && entry != null && mergedEntry.getKey() < entry.getKey())) {
                addElement(mergedEntry.getKey(), mergedEntry.getValue(), mergedEntry.getLevel(), false);
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
            } else if ((entry != null && mergedEntry == null)
                    || (mergedEntry != null && entry != null && entry.getKey() < mergedEntry.getKey())) {
                addElement(entry.getKey(), entry.getValue(), entry.getLevel(), false);
                if (it.hasNext()) {
                    entry = it.next();
                } else {
                    entry = null;
                }
            } else {
                addElement(entry.getKey(), entry.getValue() + mergedEntry.getValue(), entry.getLevel(), false);
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
                if (it.hasNext()) {
                    entry = it.next();
                } else {
                    entry = null;
                }
            }
        }
    }

    double findCoeffValue(PeekingIterator<WaveletCoefficient> it, long coeffIdx, int coeffLevel) {
        WaveletCoefficient curr = it.peek();
        if (curr == null)
            return 0.0;
        long currIdx;
        if (curr.getLevel() <= coeffLevel)
            currIdx = curr.getKey() >> (coeffLevel - curr.getLevel());
        else
            currIdx = curr.getKey() << (curr.getLevel() - coeffLevel);
        try {
            while (it.hasNext() && currIdx < coeffIdx) {
                it.next();
                curr = it.peek();
                if (curr == null)
                    return 0.0;
                if (curr.getLevel() <= coeffLevel)
                    currIdx = curr.getKey() >> (coeffLevel - curr.getLevel());
                else
                    currIdx = curr.getKey() << (curr.getLevel() - coeffLevel);
            }
        } catch (NoSuchElementException e) {
            return 0.0;
        }

        if (curr != null && currIdx == coeffIdx) {
            it.next();
            return curr.getValue();
        } else {
            return 0.0;
        }
    }

    public long convertPositionToCoeff(Long position) {
        return ((position - domainStart) >>> 1) | (1L << (maxLevel - 1));
        //return (1l << (maxLevel - 1)) + ((position >> 1) - (domainStart >> 1));
    }

    public long convertCoeffToPosition(long coeff, int level) {
        // Position is calculated via formula coeff << (maxLevel - level) - domainLength + domainStart.
        // Domain length is expanded like domainEnd-domainStart, to avoid integer overflow
        //return (coeff << level) - domainEnd + domainStart + domainStart;

        if (level == 0)
            return coeff;
        else {
            //binary mask, used to zero out most significant bits
            long mask = domainEnd - domainStart;
            return ((coeff << level) & mask) + domainStart;
        }
    }

    public DyadicTupleRange convertCoeffToSupportInterval(long coeffIndex, int level) {
        long intervalStart = convertCoeffToPosition(coeffIndex, level);
        return new DyadicTupleRange(intervalStart, intervalStart + (1L << level) - 1, 0.0);
    }

    @Override
    public double pointQuery(long position) {
        if (position == getDomainStart()) {
            return getApproximatedPrefixSum(position);
        } else {
            return getApproximatedPrefixSum(position) - getApproximatedPrefixSum(position - 1);
        }
    }

    private DyadicTupleRange getPrefixSum(PeekingIterator<WaveletCoefficient> it, double average, long startPosition,
            long startCoeffIdx, int level) {
        double coeffVal = 0.0;
        long coeffIdx = 0;
        DyadicTupleRange result = convertCoeffToSupportInterval(startPosition, 0);
        // start reverse decomposition starting from the top coefficient (i.e. with level=maxLevel)
        for (int i = level; i >= 0; i--) {
            if (i == 0) {
                //on the last level use position to calculate sign of the coefficient
                coeffIdx = startPosition - domainStart;
            } else {
                coeffIdx = startCoeffIdx >>> (i - 1);
            }
            if ((coeffIdx & 0x1) == 1) {
                coeffVal *= -1;
            }
            if (coeffVal != 0.0) {
                if (i == 0) {
                    result = new DyadicTupleRange(startPosition, startPosition, 0.0);
                } else {
                    result = convertCoeffToSupportInterval(coeffIdx, i);
                }
            }
            average += coeffVal;
            coeffVal = findCoeffValue(it, coeffIdx, i);
            if (normalize)
                coeffVal *= WaveletCoefficient.getNormalizationCoefficient(maxLevel, i);
        }
        result.setValue(average);
        return result;
    }

    private double getApproximatedPrefixSum(long position) {
        //find a prefix sum and it's support interval for given position
        DyadicTupleRange positionDyadicRange = getPrefixSum(position);
        if (linearApproximation) {
            //if a range is a single point there is not need for approximation
            if (positionDyadicRange.getStart() == positionDyadicRange.getEnd())
                return positionDyadicRange.getValue();
            //indicates whether the position is located in the first half of the appropriate dyadic range or not
            boolean firstHalf = position < (positionDyadicRange.getStart() + positionDyadicRange.getEnd()) / 2.0;
            //        double approximationXStart = (positionDyadicRange.getStart() + positionDyadicRange.getEnd()) / 2.0;
            double stairLength = (positionDyadicRange.getEnd() - positionDyadicRange.getStart()) / 2.0 + 1;
            double x = position;
            DyadicTupleRange prevDyadicRange;
            DyadicTupleRange nextDyadicRange;
            if (firstHalf) {
                nextDyadicRange = positionDyadicRange;
                if (positionDyadicRange.getStart() == domainStart) {
                    prevDyadicRange = new DyadicTupleRange(domainStart, domainStart, 0.0);
                    x += 1;
                } else {
                    prevDyadicRange = getPrefixSum(positionDyadicRange.getStart() - 1);
                    stairLength += (prevDyadicRange.getEnd() - prevDyadicRange.getStart()) / 2.0;
                }
            } else {
                prevDyadicRange = positionDyadicRange;
                if (positionDyadicRange.getEnd() == domainEnd) {
                    DyadicTupleRange prev = getPrefixSum(positionDyadicRange.getStart() - 1);
                    nextDyadicRange = new DyadicTupleRange(domainEnd, domainEnd,
                            positionDyadicRange.getValue() + (positionDyadicRange.getValue() - prev.getValue()) / 2);
                    stairLength -= 0.5;
                } else {
                    nextDyadicRange = getPrefixSum(positionDyadicRange.getEnd() + 1);
                    stairLength += (nextDyadicRange.getEnd() - nextDyadicRange.getStart()) / 2.0;
                }
            }
            x -= (prevDyadicRange.getStart() + prevDyadicRange.getEnd()) / 2.0;
            double stairHeight = nextDyadicRange.getValue() - prevDyadicRange.getValue();
            return prevDyadicRange.getValue() + x * stairHeight / stairLength;
        } else {
            return positionDyadicRange.getValue();
        }
    }

    private DyadicTupleRange getPrefixSum(long position) {
        PeekingIterator<WaveletCoefficient> it = new PeekingIterator<>(synopsisElements.iterator());
        long startCoeffIdx = convertPositionToCoeff(position);
        double mainAvg = findCoeffValue(it, 0l, maxLevel);
        return getPrefixSum(it, mainAvg, position, startCoeffIdx, maxLevel);
    }

    //    public Double rangeQuery2(Long startPosition, Long endPosition) {
    //        Double value = 0.0;
    //        PeekingIterator<? extends ISynopsisElement> it = new PeekingIterator<>(coefficients.iterator());
    //        Double mainAvg = findCoeffValue(it, 0l);
    //        List<DyadicTupleRange> workingSet = new ArrayList<>();
    //        Long convertedStartPos = convertPositionToCoeff(startPosition) << 1;
    //        Long convertedEndPos = convertPositionToCoeff(endPosition) << 1;
    //        workingSet.add(new DyadicTupleRange(startPosition - domainStart, endPosition - domainStart, mainAvg));
    //        int level = maxLevel;
    //        while (!workingSet.isEmpty()) {
    //            ListIterator<DyadicTupleRange> rangeIt = workingSet.listIterator();
    //
    //            while (rangeIt.hasNext()) {
    //                DyadicTupleRange range = rangeIt.next();
    //                long startIdx = (range.getStart() >> level) + (1l << (maxLevel - level));
    //                Double coeff = findCoeffValue(it, startIdx)
    //                        * WaveletCoefficient.getNormalizationCoefficient(maxLevel, level);
    //                if (range.getEnd() - range.getStart() + 1 == (1l << level)) {
    //                    //range is a full dyadic subrange
    //                    rangeIt.remove();
    //                    Double rangeValue = 0.0;
    //                    if (level == 0) {
    //                        rangeValue = ((startIdx & 0x1) == 0) ? range.getValue() + coeff : range.getValue() - coeff;
    //                    } else {
    //                        rangeValue = range.getValue() * (1l << level);
    //                    }
    //                    value += rangeValue;
    //                } else {
    //                    long startPos = (range.getStart() >> (level - 1)) + (1l << (maxLevel - level + 1));
    //                    long endPos = (range.getEnd() >> (level - 1)) + (1l << (maxLevel - level + 1));
    //                    long splitPos = (endPos - (1l << (maxLevel - level + 1))) << (level - 1);
    //                    //split the range
    //                    if (startPos != endPos) {
    //                        rangeIt.remove();
    //                        rangeIt.add(new DyadicTupleRange(range.getStart(), splitPos - 1, range.getValue() + coeff));
    //                        rangeIt.add(new DyadicTupleRange(splitPos, range.getEnd(), range.getValue() - coeff));
    //                    } else {
    //                        rangeIt.remove();
    //                        rangeIt.add(new DyadicTupleRange(range.getStart(), range.getEnd(),
    //                                range.getValue() + coeff * (((startPos & 0x1) == 0) ? 1 : -1)));
    //                    }
    //                }
    //            }
    //            level--;
    //        }
    //        return value;
    //    }

    @Override
    public double rangeQuery(long startPosition, long endPosition) {
        double startSum = 0.0;
        if (startPosition > getDomainStart()) {
            startSum = getApproximatedPrefixSum(startPosition - 1);
        }
        return getApproximatedPrefixSum(endPosition) - startSum;
    }

    public Double rangeQuery3(Long startPosition, Long endPosition) {
        long leftCoeffIdx = (1l << (maxLevel - 1)) + (startPosition >> 1) - (domainStart >> 1);
        long rightCoeffIdx = (1l << (maxLevel - 1)) + (endPosition >> 1) - (domainStart >> 1);
        Double value = 0.0;
        //during the query time coefficients are stored in the list, so this case is safe
        ReverseListIterator<WaveletCoefficient> it =
                new ReverseListIterator<>((List<WaveletCoefficient>) synopsisElements);
        WaveletCoefficient coeff = null;
        if (it.hasNext()) {
            coeff = it.next();
        }
        int level = 1;
        while (leftCoeffIdx > 0 && rightCoeffIdx > 0) {
            int coeffLevel = WaveletCoefficient.getLevel(coeff.getKey(), maxLevel);
            while (coeffLevel <= level && it.hasNext()) {
                if (coeff.getKey() == leftCoeffIdx) {
                    value += getLeavesNumWithinRange(coeff, startPosition, endPosition) * coeff.getValue()
                            * WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeffLevel);
                } else if (coeff.getKey() == rightCoeffIdx) {
                    value += getLeavesNumWithinRange(coeff, startPosition, endPosition) * coeff.getValue()
                            * WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeffLevel);
                }
                coeff = it.next();
                coeffLevel = WaveletCoefficient.getLevel(coeff.getKey(), maxLevel);
            }
            rightCoeffIdx >>= 1;
            leftCoeffIdx >>= 1;
            level++;
        }
        while (it.hasNext() && coeff.getKey() != 0) {
            coeff = it.next();
        }
        // take into account for root coefficient
        if (coeff.getKey() == 0)
            value += (Math.subtractExact(endPosition, startPosition) + 1) * coeff.getValue()
                    * (normalize ? WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeffLevel) : 1);
        return value;
    }

    private long getLeavesNumWithinRange(WaveletCoefficient coeff, Long rangeStart, Long rangeEnd) {
        if (WaveletCoefficient.getLevel(coeff.getKey(), maxLevel) > 1) {
            DyadicTupleRange leftChildRange = convertCoeffToSupportInterval(coeff.getKey() << 1,
                    WaveletCoefficient.getLevel(coeff.getKey(), maxLevel) - 1);
            DyadicTupleRange rightChildRange = convertCoeffToSupportInterval((coeff.getKey() << 1) + 1,
                    WaveletCoefficient.getLevel(coeff.getKey(), maxLevel) - 1);
            long leftLeaves = intersectInterval(leftChildRange.getStart(), leftChildRange.getEnd(), rangeStart,
                    rangeEnd);
            long rightLeaves = intersectInterval(rightChildRange.getStart(), rightChildRange.getEnd(), rangeStart,
                    rangeEnd);
            return leftLeaves - rightLeaves;
        } else {
            long childLeftPos = (coeff.getKey() - (1 << (maxLevel - 1)) + (domainStart >> 1)) << 1;
            if (rangeStart < childLeftPos && rangeEnd == childLeftPos
                    || rangeStart == (childLeftPos + 1) && rangeEnd > (childLeftPos + 1)) {
                return 1;
            } else
                return 0;

        }
    }

    private long intersectInterval(long i1Start, long i1End, long i2Start, long i2End) {
        if (i2Start > i1End && i2Start > i1Start || i1Start > i2End && i1End >= i2End) {
            return 0;
        } else {
            return Math.min(i2End, i1End) - Math.max(i1Start, i2Start) + 1;
        }
    }

}