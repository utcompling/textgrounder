/**
 * Copyright 2009 Rednaxela
 *
 * This software is provided 'as-is', without any express or implied
 * warranty. In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 *    1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 *
 *    2. This notice may not be removed or altered from any source
 *    distribution.
 */

package ags.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;

/**
 * An efficient well-optimized kd-tree
 *
 * @author Rednaxela
 */
public class KdTree {
    // split method enum
    public enum SplitMethod { HALFWAY, MEDIAN, MAX_MARGIN }

    // All types
    private final int                  dimensions;
    public final KdTree                parent;
    private int                        bucketSize;
    private SplitMethod                splitMethod;

    // Leaf only
    private double[][]                 locations;
    private int                        locationCount;

    // Stem only
    private KdTree                     left, right;
    private int                        splitDimension;
    private double                     splitValue;

    // Bounds
    public double[]                    minLimit, maxLimit;
    private boolean                    singularity;

    /**
     * Construct a KdTree with a given number of dimensions and a limit on
     * maxiumum size (after which it throws away old points)
     */
    public KdTree(int dimensions, int bucketSize, SplitMethod splitMethod) {
        this.bucketSize = bucketSize;
        this.dimensions = dimensions;
        this.splitMethod = splitMethod;

        // Init as leaf
        this.locations = new double[bucketSize][];
        this.locationCount = 0;
        this.singularity = true;

        // Init as root
        this.parent = null;
    }

    /**
     * Constructor for child nodes. Internal use only.
     */
    private KdTree(KdTree parent, boolean right) {
        this.dimensions = parent.dimensions;
        this.bucketSize = parent.bucketSize;
        this.splitMethod = parent.splitMethod;

        // Init as leaf
        this.locations = new double[Math.max(bucketSize, parent.locationCount)][];
        this.locationCount = 0;
        this.singularity = true;

        // Init as non-root
        this.parent = parent;
    }

    /**
     * Get the number of points in the tree
     */
    public int size() {
        return locationCount;
    }

    public KdTree getLeaf(double[] location) {
        if (left == null || right == null)
            return this;
        else if (location[splitDimension] <= splitValue)
            return left.getLeaf(location);
        else
            return right.getLeaf(location);
    }

    /**
     * Add a point and associated value to the tree
     */
    public void addPoint(double[] location) {
        if (locationCount >= locations.length) {
            double[][] newLocations = new double[locations.length * 2][];
            System.arraycopy(locations, 0, newLocations, 0, locationCount);
            locations = newLocations;
        }

        locations[locationCount] = location;
        locationCount++;
        extendBounds(location);
    }

    /**
     * Extends the bounds of this node do include a new location
     */
    private final void extendBounds(double[] location) {
        if (minLimit == null) {
            minLimit = new double[dimensions];
            System.arraycopy(location, 0, minLimit, 0, dimensions);
            maxLimit = new double[dimensions];
            System.arraycopy(location, 0, maxLimit, 0, dimensions);
            return;
        }

        for (int i = 0; i < dimensions; i++) {
            if (Double.isNaN(location[i])) {
                minLimit[i] = Double.NaN;
                maxLimit[i] = Double.NaN;
                singularity = false;
            }
            else if (minLimit[i] > location[i]) {
                minLimit[i] = location[i];
                singularity = false;
            }
            else if (maxLimit[i] < location[i]) {
                maxLimit[i] = location[i];
                singularity = false;
            }
        }
    }

    private List<double[]> getLocations() {
        LinkedList<double[]> l = new LinkedList<double[]>();
        getLocationsHelper(l);
        return l;
    }

    private void getLocationsHelper(List<double[]> l) {
        if (left == null || right == null) {
            for (int i=0; i<locationCount; i++) {
                l.add(locations[i]);
            }
        } else {
            left.getLocationsHelper(l);
            right.getLocationsHelper(l);
        }
    }

    public void annihilateData() {
        // assuming we are using a static KD tree, once we have
        // generated the entire tree and have the split locations,
        // then we no longer need to store all the points. Use
        // this to clean everything.
        if (left != null) left.annihilateData();
        if (right != null) right.annihilateData();
        locations = null;
    }

    /**
     * Find the widest axis of the bounds of this node
     */
    private final int findWidestAxis() {
        int widest = 0;
        double width = (maxLimit[0] - minLimit[0]) * getAxisWeightHint(0);
        if (Double.isNaN(width)) width = 0;
        for (int i = 1; i < dimensions; i++) {
            double nwidth = (maxLimit[i] - minLimit[i]) * getAxisWeightHint(i);
            if (Double.isNaN(nwidth)) nwidth = 0;
            if (nwidth > width) {
                widest = i;
                width = nwidth;
            }
        }
        return widest;
    }

    public List<KdTree> getNodes() {
        List<KdTree> list = new ArrayList<KdTree>();
        this.getNodesHelper(list);
        return list;
    }

    private void getNodesHelper(List<KdTree> list) {
        list.add(this);
        if (left != null) left.getNodesHelper(list);
        if (right != null) right.getNodesHelper(list);
    }

    public List<KdTree> getLeaves() {
        List<KdTree> list = new ArrayList<KdTree>();
        this.getLeavesHelper(list);
        return list;
    }

    private void getLeavesHelper(List<KdTree> list) {
        if (left == null && right == null)
            list.add(this);
        else{
            if (left != null)
                left.getLeavesHelper(list);
            if (right != null)
                right.getLeavesHelper(list);
        }
    }

    public void balance() {
        nodeSplit(this);
    }

    private void nodeSplit(KdTree cursor) {
        if (cursor.locationCount > cursor.bucketSize) {
            cursor.splitDimension = cursor.findWidestAxis();

            if (splitMethod == SplitMethod.HALFWAY) {
                cursor.splitValue = (cursor.minLimit[cursor.splitDimension] +
                                     cursor.maxLimit[cursor.splitDimension]) * 0.5;
            } else if (splitMethod == SplitMethod.MEDIAN) {
                // split on the median of the elements
                List<Double> list = new ArrayList<Double>();
                for(int i=0; i<cursor.locationCount ; i++) {
                    list.add(cursor.locations[i][cursor.splitDimension]);
                }
                Collections.sort(list);
                if(list.size() % 2 == 1) {
                    cursor.splitValue = list.get(list.size() / 2);
                } else {
                    cursor.splitValue = (list.get(list.size() / 2) + list.get(list.size() / 2 - 1))/2;
                }
            } else if (splitMethod == SplitMethod.MAX_MARGIN) {
                List<Double> list = new ArrayList<Double>();
                for(int i = 0; i < cursor.locationCount; i++) {
                    list.add(cursor.locations[i][cursor.splitDimension]);
                }
                Collections.sort(list);
                double maxMargin = 0.0;
                double splitValue = Double.NaN;
                for (int i = 0; i < list.size() - 1; i++) {
                    double delta = list.get(i+1) - list.get(i);
                    if (delta > maxMargin) {
                        maxMargin = delta;
                        splitValue = list.get(i) + 0.5 * delta;
                    }
                }
                cursor.splitValue = splitValue;
            }

            // Never split on infinity or NaN
            if (cursor.splitValue == Double.POSITIVE_INFINITY) {
                cursor.splitValue = Double.MAX_VALUE;
            }
            else if (cursor.splitValue == Double.NEGATIVE_INFINITY) {
                cursor.splitValue = -Double.MAX_VALUE;
            }
            else if (Double.isNaN(cursor.splitValue)) {
                cursor.splitValue = 0;
            }

            // Don't split node if it has no width in any axis. Double the
            // bucket size instead
            if (cursor.minLimit[cursor.splitDimension] == cursor.maxLimit[cursor.splitDimension]) {
                double[][] newLocations = new double[cursor.locations.length * 2][];
                System.arraycopy(cursor.locations, 0, newLocations, 0, cursor.locationCount);
                cursor.locations = newLocations;
                return;
            }

            // Don't let the split value be the same as the upper value as
            // can happen due to rounding errors!
            if (cursor.splitValue == cursor.maxLimit[cursor.splitDimension]) {
                cursor.splitValue = cursor.minLimit[cursor.splitDimension];
            }

            // Create child leaves
            KdTree left = new ChildNode(cursor, false);
            KdTree right = new ChildNode(cursor, true);

            // Move locations into children
            for (int i = 0; i < cursor.locationCount; i++) {
               double[] oldLocation = cursor.locations[i];
               if (oldLocation[cursor.splitDimension] > cursor.splitValue) {
                   // Right
                   right.locations[right.locationCount] = oldLocation;
                   right.locationCount++;
                   right.extendBounds(oldLocation);
               }
               else {
                   // Left
                   left.locations[left.locationCount] = oldLocation;
                   left.locationCount++;
                   left.extendBounds(oldLocation);
               }
            }

            // Make into stem
            cursor.left = left;
            cursor.right = right;
            cursor.locations = null;
            cursor.nodeSplit(left);
            cursor.nodeSplit(right);
        }
    }


    protected double pointDist(double[] p1, double[] p2) {
        double d = 0;

        for (int i = 0; i < p1.length; i++) {
            double diff = (p1[i] - p2[i]);
            if (!Double.isNaN(diff)) {
                d += diff * diff;
            }
        }

        return d;
    }

    protected double pointRegionDist(double[] point, double[] min, double[] max) {
        double d = 0;

        for (int i = 0; i < point.length; i++) {
            double diff = 0;
            if (point[i] > max[i]) {
                diff = (point[i] - max[i]);
            }
            else if (point[i] < min[i]) {
                diff = (point[i] - min[i]);
            }

            if (!Double.isNaN(diff)) {
                d += diff * diff;
            }
        }

        return d;
    }

    protected double getAxisWeightHint(int i) {
        return 1.0;
    }

    /**
     * Internal class for child nodes
     */
    private class ChildNode extends KdTree {
        private ChildNode(KdTree parent, boolean right) {
            super(parent, right);
        }

        // Distance measurements are always called from the root node
        protected double pointDist(double[] p1, double[] p2) {
            throw new IllegalStateException();
        }

        protected double pointRegionDist(double[] point, double[] min, double[] max) {
            throw new IllegalStateException();
        }
    }

    private static String darrayToString(double[] array) {
        String retval = "";
        for (int i = 0; i < array.length; i++) {
            retval += array[i] + " ";
        }
        return retval;
    }

}

