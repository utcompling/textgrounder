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

import java.util.*;

public class Location extends SmallLocation {

    protected String name;
    protected String type;
    protected int pop;
    protected String container;
    /**
     * List of pack pointers into the DocumentSet so that context (snippets) can be extracted
     */
    protected ArrayList<Integer> backPointers;

    public Location() {
    }

    public Location(int id, String name, String type, Coordinate coord, int pop,
          String container, int count) {
        super();
        this.id = id;
        this.name = name;
        this.type = type;
        this.coord = coord;
        this.pop = pop;
        this.container = container;
        this.count = count;
    }

    @Override
    public boolean looselyMatches(SmallLocation other, double maxDiff) {
        /*if(!this.name.equals(other.name)) {
        System.out.println(this.name);
        System.out.println(other.name);
        }*/
        return this.name.equals(other.getName()) && this.coord.looselyMatches(other.coord, maxDiff);
    }

    @Override
    public String getName() {
        return name;
    }

    /*    public Location(int id, String name, String type, double lon, double lat, int pop, String container, int count) {
    Location(id, name, type, new Coordinate(lon, lat), pop, container, count);
    }*/
    @Override
    public String toString() {
        return id + ", " + name + ", " + type + ", (" + coord + "), " + pop + ", " + container;
    }

    /**
     * @return the backPointers
     */
    @Override
    public ArrayList<Integer> getBackPointers() {
        return backPointers;
    }

    /**
     * @param backPointers the backPointers to set
     */
    @Override
    public void setBackPointers(ArrayList<Integer> backPointers) {
        this.backPointers = backPointers;
    }

    /**
     * @return the container
     */
    @Override
    public String getContainer() {
        return container;
    }

    /**
     * @param container the container to set
     */
    @Override
    public void setContainer(String container) {
        this.container = container;
    }

    /**
     * @return the pop
     */
    @Override
    public int getPop() {
        return pop;
    }

    /**
     * @param pop the pop to set
     */
    @Override
    public void setPop(int pop) {
        this.pop = pop;
    }

    /**
     * @return the type
     */
    @Override
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    @Override
    public void setType(String type) {
        this.type = type;
    }

    /**
     * @param name the name to set
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }
}
