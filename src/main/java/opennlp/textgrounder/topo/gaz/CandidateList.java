///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.topo.gaz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import opennlp.textgrounder.topo.Coordinate;
import opennlp.textgrounder.topo.Location;

/**
 * Represents a list of candidates for a given toponym.
 *
 * @author Travis Brown <travis.brown@mail.utexas.edu>
 */
public abstract class CandidateList implements Iterable<Location> {
  /**
   * The size of the list.
   */
  public abstract int size();

  /**
   * The location at a given index.
   */
  public abstract Location get(int index);

  /**
   * Iterate over the candidates in the list in order of increasing distance
   * from a given point. This should be overridden in subclasses where the
   * structure of the gazetteer makes a more efficient implementation
   * possible.
   */
  public Iterator<SortedItem> getNearest(Coordinate point) {
    List<SortedItem> items = new ArrayList<SortedItem>(this.size());
    for (int i = 0; i < this.size(); i++) {
      items.add(new SortedItem(i, this.get(i).getRegion().distance(point)));
    }
    Collections.sort(items);
    return items.iterator();
  }

  /**
   * Represents an item in the list sorted with respect to distance from a
   * given coordinate.
   */
  public class SortedItem implements Comparable<SortedItem> {
    private final int index;
    private final double distance;

    private SortedItem(int index, double distance) {
      this.index = index;
      this.distance = distance;
    }

    /**
     * The index of this candidate in the list.
     */
    public int getIndex() {
      return this.index;
    }

    /**
     * The distance between this candidate and the given point.
     */
    public double getDistance() {
      return this.distance;
    }

    /**
     * A convenience method providing direct access to the location object for
     * this candidate.
     */
    public Location getLocation() {
      return CandidateList.this.get(this.index);
    }

    /**
     * Returns a point in the region for the location coordinate. When
     * possible for region locations this should be the point in the region
     * nearest to the given point (in which case this implementation should be
     * overridden).
     */
    public Coordinate getCoordinate() {
      return CandidateList.this.get(this.index).getRegion().getCenter();
    }

    /**
     * We need to be able to sort by distance, with the lower index coming
     * first in the case that the distances are the same.
     */
    public int compareTo(SortedItem other) {
      double diff = this.getDistance() - other.getDistance();
      if (diff < 0.0) {
        return -1;
      } else if (diff > 0.0) {
        return 1;
      } else {
        return this.getIndex() - other.getIndex();
      }   
    }
  }

  /**
   * A convenience class that we can extend in subclasses to avoid repeatedly
   * implementing the unsupported removal operation.
   */
  private abstract class CandidateListIterator {
    public void remove() {
      throw new UnsupportedOperationException(
        "Cannot remove a location from a candidate list."
      );
    }
  }
}

