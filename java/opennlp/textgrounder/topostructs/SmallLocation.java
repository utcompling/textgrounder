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
package opennlp.textgrounder.topostructs;

import java.io.*;
import java.util.ArrayList;

/**
 * A simple class for encapsulating a location on a sphere along with (1) a
 * count of how many times a location has been seen in a text; (2) a list of
 * places where the location was seen.
 * <p>
 * The following properties define the location itself:
 * <ul>
 * <li>A Coordinate object specifying the latitude and longitude
 * <li>An integer identifying the location, for fast storage/lookup in tables
 * <li>An integer identifying the name of the location
 * </ul>
 * <p>
 * The methods are mostly getters and setters. The only interesting method is
 * looselyMatches(), which compares two locations approximately, allowing them
 * to differ slightly in position.
 * 
 * @author Taesun Moon
 * 
 */
public class SmallLocation implements Serializable {

    static private final long serialVersionUID = 13371511L;
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

    public SmallLocation() {
    }

    public SmallLocation(int _id, int _nameid, Coordinate _coord, int _count) {
        id = _id;
        nameid = _nameid;
        coord = _coord;
        count = _count;
    }

    public SmallLocation(int _id, String _name, String _type, Coordinate _coord,
          int _pop, String _container, int _count) {
        throw new UnsupportedOperationException();
    }

    public String getName() {
        throw new UnsupportedOperationException();
    }

    /**
     * Compare two locations approximately, allowing the latitude or longitutde
     * to differ by up to `maxDiff'. FIXME: This also requires that the name
     * ID's are the same, which seems to defeat the whole purpose of allowing
     * approximate matching on the coordinate.
     * 
     * @param other
     * @param maxDiff
     * @return
     */
    public boolean looselyMatches(SmallLocation other, double maxDiff) {
        return this.nameid == other.nameid && this.coord.looselyMatches(other.coord, maxDiff);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SmallLocation other = (SmallLocation) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }

    public double computeDistanceTo(SmallLocation other) {
        return this.coord.computeDistanceTo(other.coord);
    }

    /**
     * @return the _nameid
     */
    public int getNameid() {
        return nameid;
    }

    /**
     * @param _nameid the _nameid to set
     */
    public void setNameid(int _nameid) {
        nameid = _nameid;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return the coord
     */
    public Coordinate getCoord() {
        return coord;
    }

    /**
     * @param coord the coord to set
     */
    public void setCoord(Coordinate coord) {
        this.coord = coord;
    }

    /**
     * @return the backPointers
     */
    public ArrayList<Integer> getBackPointers() {
        return backPointers;
    }

    /**
     * @param backPointers the backPointers to set
     */
    public void setBackPointers(ArrayList<Integer> backPointers) {
        this.backPointers = backPointers;
    }

    /**
     * @return the container
     */
    public String getContainer() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param container the container to set
     */
    public void setContainer(String container) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the pop
     */
    public int getPop() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pop the pop to set
     */
    public void setPop(int pop) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the type
     */
    public String getType() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the count
     */
    public double getCount() {
        return count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(double count) {
        this.count = count;
    }
}
