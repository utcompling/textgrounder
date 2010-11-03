///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
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
package opennlp.textgrounder.old.topostructs;

import java.io.Serializable;
import java.util.*;

/**
 * A class encapsulating a location on a sphere along with various properties.
 * This is an extension of SmallLocation and adds a number of new properties:
 * <ul>
 * <li>The name of the location
 * <li>The type of the location (FIXME which types can occur?)
 * <li>The location's population
 * <li>The container of the location (FIXME document this)
 * </ul>
 * <p>
 * The methods of this class are basically uninteresting -- mostly just getter
 * and setter methods.
 * 
 * @author Taesun Moon
 * 
 */
public class Location implements Serializable, Comparable<Location> {

    protected String name;
    protected String type;
    protected int pop;
    protected String container;
    protected int id;
    protected int nameid;
    protected Coordinate coord;
    /**
     * Counts of location in given text. Is double type to accomodate
     * hyperparameters and fractional counts;
     */
    protected double count;
    /**
     * List of back pointers into the DocumentSet so that context (snippets) can be extracted
     */
    protected ArrayList<Integer> backPointers = null;

    public Location() {
    }

    public Location(int id, String name, String type, Coordinate coord, int pop) {
      this(id, name, type, coord, pop, null);
    }

    public Location(int id, String name, String type, Coordinate coord, int pop,
          String container) {
      this(id, name, type, coord, pop, container, 0);
    }

    public Location(int id, String name, String type, Coordinate coord, int pop,
          String container, int count) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.coord = coord;
        this.pop = pop;
        this.container = container;
        this.count = count;
    }

    /**
     * Override looselyMatches() so that the names must compare the same. FIXME:
     * This has the same problem as is documented in the superclass method of
     * the same name.
     */
    public boolean looselyMatches(Location other, double maxDiff) {
        /*if(!this.name.equals(other.name)) {
        System.out.println(this.name);
        System.out.println(other.name);
        }*/
        return this.name.equals(other.getName()) && this.coord.looselyMatches(other.coord, maxDiff);
    }

    public String getName() {
        return name;
    }

    public double computeDistanceTo(Location other) {
        return this.coord.computeDistanceTo(other.coord);
    }

    /**
     * @return the container
     */
    public String getContainer() {
        return container;
    }

    /**
     * @param container the container to set
     */
    public void setContainer(String container) {
        this.container = container;
    }

    /**
     * @return the pop
     */
    public int getPop() {
        return pop;
    }

    /**
     * @param pop the pop to set
     */
    public void setPop(int pop) {
        this.pop = pop;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<Integer> getBackPointers() {
        return backPointers;
    }

    public void setBackPointers(ArrayList<Integer> backPointers) {
        this.backPointers = backPointers;
    }

    public Coordinate getCoord() {
        return coord;
    }

    public void setCoord(Coordinate coord) {
        this.coord = coord;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNameid() {
        return nameid;
    }

    public void setNameid(int nameid) {
        this.nameid = nameid;
    }

    /*    public Location(int id, String name, String type, double lon, double lat, int pop, String container, int count) {
    Location(id, name, type, new Coordinate(lon, lat), pop, container, count);
    }*/

    @Override
    public String toString() {
      return String.format("%d, %s, %s, (%s), %d, %s", id, name, type, coord, pop, container);
    }

    @Override
    public int hashCode() {
      return id;
    }

    /**
     * Two Locations are the same if they have the same class and same ID.
     */
    @Override
    public boolean equals(Object other) {
      return other != null &&
             other.getClass() == this.getClass() &&
             ((Location) other).id == this.id;
    }

    public int compareTo(Location other) {
      return this.id - other.id;
    }
}

