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
package org.apache.hyracks.storage.am.statistics.sketch.quantile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

import com.google.common.collect.Iterators;

/**
 * Implementation of modified adaptive Greenwald-Khanna sketch from "Quantiles over data streams: An Experimental Study"
 */
public class QuantileSketch<T extends Comparable<T>> {

    class QuantileSketchElement implements Comparable<QuantileSketchElement> {
        private final T value;
        private long g;
        private final long delta;

        public QuantileSketchElement(T value, long g, long delta) {
            this.value = value;
            this.g = g;
            this.delta = delta;
        }

        @Override
        public int compareTo(QuantileSketchElement o) {
            if (this == o)
                return 0;
            else
                return 1;
        }

        @Override
        public String toString() {
            return new StringBuilder().append("V=").append(value).append(", g=").append(g).append(", Δ=").append(delta)
                    .toString();
        }
    }

    class ThresholdEntry {
        private QuantileSketchElement sketchElement;
        private double threshold;

        public ThresholdEntry(QuantileSketchElement sketchElement, double threshold) {
            this.sketchElement = sketchElement;
            this.threshold = threshold;
        }
    }

    class TreeMapWithDuplicates<K, V> {
        private final TreeMap<K, LinkedList<V>> map;

        TreeMapWithDuplicates() {
            map = new TreeMap<>();
        }

        public void put(K key, V value) {
            if (!map.containsKey(key)) {
                map.put(key, new LinkedList<>(Arrays.asList(value)));
            } else {
                map.get(key).add(value);
            }

        }

        public V higherEntry(K key) {
            if (map.higherEntry(key) == null)
                return map.get(key).getLast();
            return map.higherEntry(key).getValue().getFirst();
        }

        public boolean containsKey(K key) {
            return map.containsKey(key);
        }

        public V next(K key, V prevValue) {
            V value;
            if (map.get(key).size() > 1 && !map.get(key).getLast().equals(prevValue)) {
                Iterator<V> it = map.get(key).iterator();
                while (it.hasNext() && !it.next().equals(prevValue));
                value = it.next();
            } else {
                value = map.higherEntry(key).getValue().getLast();
            }
            return value;
        }

        public void remove(K key, V value) {
            if (map.get(key).size() > 1) {
                map.get(key).remove(value);
            } else {
                map.remove(key);
            }
        }

        public Iterator<V> iterator() {
            List<Iterator<V>> iterators = new ArrayList<>();
            for (List<V> list : map.values()) {
                iterators.add(list.iterator());
            }
            return Iterators.concat(iterators.iterator());
        }
    }

    private final double accuracy;
    private final TreeMapWithDuplicates<T, QuantileSketchElement> elements;
    private final Queue<ThresholdEntry> compressibleElements;
    private int size;

    public QuantileSketch(double accuracy, T domainEnd) {
        this.accuracy = accuracy;
        elements = new TreeMapWithDuplicates<>();
        //max heap to store elements thresholds
        compressibleElements = new PriorityQueue<>(
                (Comparator<ThresholdEntry>) (o1, o2) -> Double.compare(o2.threshold, o1.threshold));
        elements.put(domainEnd, new QuantileSketchElement(domainEnd, 1, 0));
    }

    public void add(T value) {
        size++;
        // find successor quantile element
        QuantileSketchElement prev = elements.higherEntry(value);

        long threshold = (long) Math.floor(accuracy * size * 2);
        long newDelta = prev.g + prev.delta;
        if (newDelta < threshold) {
            // merge new element into prev right away. Since new element is (value,1,Δ) prev's g is simply incremented
            prev.g++;
        } else {
            QuantileSketchElement newElement = new QuantileSketchElement(value, 1, newDelta - 1);
            elements.put(value, newElement);
            // add entry to priority queue. Entry's threshold is calculated as gi + gi+1 + Δ = newDelta + 1
            compressibleElements.offer(new ThresholdEntry(newElement, newDelta + 1));
            while (true) {
                ThresholdEntry maxThresholdElement = compressibleElements.peek();
                if (maxThresholdElement.threshold > threshold) {
                    // all elements are greater then threshold. simply break because the new element was already added
                    break;
                }
                // update next element's threshold
                maxThresholdElement = compressibleElements.poll();
                QuantileSketchElement nextElement =
                        elements.next(maxThresholdElement.sketchElement.value, maxThresholdElement.sketchElement);
                long newThreshold =
                        maxThresholdElement.sketchElement.g + maxThresholdElement.sketchElement.delta + nextElement.g;
                if (newThreshold <= threshold) {
                    maxThresholdElement.sketchElement.g += nextElement.g;
                    elements.remove(nextElement.value, nextElement);
                    break;
                } else {
                    maxThresholdElement.threshold = newThreshold;
                    compressibleElements.offer(maxThresholdElement);
                }
            }
        }
    }

    // returns max_i (gi+Δi) after all elements were added to the quantile summary
    public long calculateMaxError() {
        long maxError = 0;
        Iterator<QuantileSketchElement> it = elements.iterator();
        while (it.hasNext()) {
            QuantileSketchElement e = it.next();
            if (e.g + e.delta > maxError) {
                maxError = e.g + e.delta;
            }
        }
        return maxError / 2;
    }

    public List<T> extractAllRanks(int quantileNum, T domainStart, long maxError) {
        List<T> ranks = new ArrayList<>(quantileNum);
        Iterator<QuantileSketchElement> it = elements.iterator();
        int quantile = 1;
        long nextRank = (long) Math.ceil(((double) size) / quantileNum);
        // TODO: substitute accuracy to the actual maximum error (i.e. max_i (gi+Δi)) observed in the quantile summary
        QuantileSketchElement prev = null;
        QuantileSketchElement e = it.hasNext() ? it.next() : null;
        if (e == null) {
            return ranks;
        }
        long rankMax = e.g;
        while (true) {
            if (rankMax + e.delta > nextRank + maxError) {
                if (prev != null) {
                    ranks.add(prev.value);
                } else {
                    ranks.add(domainStart);
                }
                nextRank = (long) Math.ceil(((double) size * ++quantile) / quantileNum);
            } else if (it.hasNext()) {
                prev = e;
                e = it.next();
                rankMax += e.g;
            } else {
                break;
            }
        }
        if (prev != null) {
            // add the last element, since it marks the element with rank = 1
            ranks.add(e.value);
        }
        return ranks;
    }

    // Original GK algorithm implementation
    /*private final double accuracy;
    private final List<QuantileSketchElement> elements;
    private final int compressThreshold;
    private int count;
    
    public ApproximateQuantileSketchBuilder(EquiWidthHistogramSynopsis synopsis, boolean isAntimatter,
            IFieldExtractor fieldExtractor, double accuracy) {
        super(synopsis, isAntimatter, fieldExtractor);
        // accuracy guarantee, which restricts the rank to r±ε*N where N is the size of the relation
        this.accuracy = accuracy;
        this.elements = new LinkedList<>();
        this.count = 0;
        this.compressThreshold = (int) Math.floor(1.0 / (2.0 * accuracy));
    }
    
    private boolean isInitialPhase() {
        return count < compressThreshold;
    }
    
    @Override
    public void addValue(long value) {
        // traverse existing sample and insert a new element
        ListIterator<QuantileSketchElement> elementsIt = elements.listIterator();
        boolean inserted = false;
        double delta;
        if (isInitialPhase()) {
            delta = 0.0;
        } else {
            delta = Math.floor(2 * accuracy * count);
        }
        QuantileSketchElement newElement = new QuantileSketchElement(value, 1, delta);
    
        while (elementsIt.hasNext()) {
            QuantileSketchElement e = elementsIt.next();
            if (value <= e.value) {
                elementsIt.previous();
                elementsIt.add(newElement);
                inserted = true;
                break;
            }
        }
        // special case, the value is higher then all current elements
        if (!inserted) {
            elementsIt.add(newElement);
        }
        count++;
        // compress the sketch when a multiple of compressThreshold elements are added
        if (count % compressThreshold == 0) {
            compress();
        }
    }
    
    private void compress() {
        int mergeThreshold = (int) Math.floor(2 * accuracy * count);
        int[] currentBandBorders = computeBandBorders(mergeThreshold);
    
        Iterator<QuantileSketchElement> elementsIt = new ReverseListIterator<>(elements);
        if (!elementsIt.hasNext()) {
            // elements have a single entry
            return;
        }
        QuantileSketchElement head = elementsIt.next();
        while (elementsIt.hasNext()) {
            QuantileSketchElement e = elementsIt.next();
            int band1 = binaryRangeSearch(currentBandBorders, head.delta);
            int band2 = binaryRangeSearch(currentBandBorders, e.delta);
            if (band1 <= band2 && (e.g + head.g + head.delta) < mergeThreshold) {
                head.setG(e.g + head.g);
                elementsIt.remove();
                count--;
            } else {
                head = e;
            }
        }
    }
    
    private int binaryRangeSearch(int[] borders, double value) {
        int start = 0;
        int step = borders.length / 2;
        while (step != 1) {
            step /= 2;
            if (borders[start] == value) {
                return start;
            } else if (borders[start] > value) {
                start += step;
            } else {
                start -= step;
            }
        }
        return start;
    }
    
    private int[] computeBandBorders(int mergeThreshold) {
        int alpha = (int) Math.ceil(Math.log(mergeThreshold) / Math.log(2));
        int[] borders = new int[alpha + 1];
        for (int i = alpha; i >= 0; i--) {
            int twoInPowerAlpha = 1 << i;
            borders[i + 1] = mergeThreshold - (1 << i) - twoInPowerAlpha - (mergeThreshold % twoInPowerAlpha);
        }
        borders[0] = mergeThreshold;
        return borders;
    }*/
}
