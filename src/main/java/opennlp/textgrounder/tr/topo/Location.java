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
package opennlp.textgrounder.tr.topo;

import java.io.Serializable;
import java.util.*;

public class Location implements Serializable {

  private static final long serialVersionUID = 42L;

  public enum Type {
    STATE, WATER, CITY, SITE, PARK, TRANSPORT, MOUNTAIN, UNDERSEA, FOREST, UNKNOWN
  }

  private final int id;
  private final String name;
  private Region region;
  private Location.Type type;
  private int population;
  private String admin1code;
  private double threshold;

    public Location(int id, String name, Region region, String typeString, Integer population, String admin1code) {
        this(id, name, region, Location.convertTypeString(typeString),
             population==null?0:population, admin1code==null?"00":admin1code, 10.0);
    }

    public Location(String idWithC, String name, Region region, String typeString, Integer population, String admin1code) {
        this(Integer.parseInt(idWithC.substring(1)), name, region, typeString, population, admin1code);
    }

    public Location(int id, String name, Region region, Location.Type type, int population, String admin1code, double threshold) {
    this.id = id;
    this.name = name;
    this.region = region;
    this.type = type;
    this.population = population;
    this.admin1code = admin1code;
    this.threshold = threshold;
  }

  public Location(int id, String name, Region region, Location.Type type, int population) {
      this(id, name, region, type, population, "00", 10.0);
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

    /*public void removeNaNs() {
        List<Coordinate> reps = this.getRegion().getRepresentatives();
        int prevSize = reps.size();
        reps = Coordinate.removeNaNs(reps);
        if(reps.size() < prevSize) {
            this.getRegion().setRepresentatives(reps);
            System.out.println("Recalculating centroid");
            this.getRegion().setCenter(Coordinate.centroid(reps));
        }
        }*/

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

    public void setType(Location.Type type) {
        this.type = type;
    }

  public Location.Type getType() {
    return this.type;
  }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public double getThreshold() {
        return this.threshold;
    }

    public void recomputeThreshold() {
        // Commented out for now since experiments with this didn't perform well
        /*if(this.getRegion().getRepresentatives().size() > 1) {
            //int count = 0;
            double minDist = Double.POSITIVE_INFINITY;
            for(int i = 0; i < this.getRegion().getRepresentatives().size(); i++) {
                for(int j = 0; j < this.getRegion().getRepresentatives().size(); j++) {
                    if(i != j) {
                        double dist = this.getRegion().getRepresentatives().get(i).distanceInKm(
                                      this.getRegion().getRepresentatives().get(j));
                        if(dist < minDist)
                            minDist = dist;
                        //count++;
                    }
                }
            }
            //dist /= count;
            this.setThreshold(minDist / 2);
            }*/
    }

    public static Location.Type convertTypeString(String typeString) {
        typeString = typeString.toUpperCase();
        if(typeString.equals("STATE"))
            return Location.Type.STATE;
        if(typeString.equals("WATER"))
            return Location.Type.WATER;
        if(typeString.equals("CITY"))
            return Location.Type.CITY;
        if(typeString.equals("SITE"))
            return Location.Type.SITE;
        if(typeString.equals("PARK"))
            return Location.Type.PARK;
        if(typeString.equals("TRANSPORT"))
            return Location.Type.TRANSPORT;
        if(typeString.equals("MOUNTAIN"))
            return Location.Type.MOUNTAIN;
        if(typeString.equals("UNDERSEA"))
            return Location.Type.UNDERSEA;
        if(typeString.equals("FOREST"))
            return Location.Type.FOREST;
        else
            return Location.Type.UNKNOWN;

    }

  public int getPopulation() {
    return this.population;
  }

  public String getAdmin1Code() {
    return admin1code;
  }

  public double distance(Location other) {
    return this.getRegion().distance(other.getRegion());
  }

  public double distanceInKm(Location other) {
    return this.getRegion().distanceInKm(other.getRegion());
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

  @Override
  public int hashCode() {
    return this.id;
  }
}

