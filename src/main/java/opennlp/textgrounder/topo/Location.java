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
package opennlp.textgrounder.topo;

import java.io.Serializable;
import java.util.*;

public class Location implements Serializable {
  public enum Type {
    STATE, WATER, CITY, SITE, PARK, TRANSPORT, MOUNTAIN, UNDERSEA, FOREST, UNKNOWN
  }

  private final int id;
  private final String name;
  private Region region;
  private final Location.Type type;
  private final int population;

  public Location(int id, String name, Region region, Location.Type type, int population) {
    this.id = id;
    this.name = name;
    this.region = region;
    this.type = type;
    this.population = population;
  }

  public Location(int id, String name, Region region, Location.Type type) {
    this(id, name, region, type, 0);
  }

  public Location(int id, String name, Region region) {
    this(id, name, region, Location.Type.UNKNOWN);
  }

  public Location(String name, Region region) {
    this(-1, name, region);
  }

  public Location(String name, Region region, Location.Type type) {
    this(-1, name, region, type);
  }

  public Location(String name, Region region, Location.Type type, int population) {
    this(-1, name, region, type, population);
  }

  public int getId() {
    return this.id;
  }

  public String getName() {
    return this.name;
  }

  public Region getRegion() {
    return this.region;
  }

  public void setRegion(Region region) {
    this.region = region;
  }

  public Location.Type getType() {
    return this.type;
  }

  public int getPopulation() {
    return this.population;
  }

  public double distance(Location other) {
    return this.getRegion().distance(other.getRegion());
  }

  @Override
  public String toString() {
    return String.format("%8d (%s), %s, (%s), %d", this.id, this.name, this.type, this.region.getCenter(), this.population);
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
}

