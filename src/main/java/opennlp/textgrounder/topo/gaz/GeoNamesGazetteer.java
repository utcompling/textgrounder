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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import opennlp.textgrounder.topo.Coordinate;
import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.topo.PointRegion;
import opennlp.textgrounder.topo.PointSetRegion;
import opennlp.textgrounder.topo.Region;
import opennlp.textgrounder.topo.SphericalGeometry;
import opennlp.textgrounder.util.cluster.Clusterer;
import opennlp.textgrounder.util.cluster.KMeans;

public class GeoNamesGazetteer implements Gazetteer {
  private final double pointRatio;
  private final int minPoints;
  private final int maxPoints;
  private final int maxConsidered;

  private final List<Location> locations;
  private final Map<String, List<Location>> names;
  private final Map<String, Integer> ipes;
  private final Map<String, Integer> adms;
  private final Map<String, List<Coordinate>> ipePoints;
  private final Map<String, List<Coordinate>> admPoints;

  public GeoNamesGazetteer(BufferedReader reader) throws IOException {
    this(reader, 0.01);
  }

  public GeoNamesGazetteer(BufferedReader reader, double pointRatio)
    throws IOException {
    this(reader, pointRatio, 5, 50);
  }

  public GeoNamesGazetteer(BufferedReader reader, double pointRatio, int minPoints, int maxPoints)
    throws IOException {
    this(reader, pointRatio, minPoints, maxPoints, 2000);
  }

  public GeoNamesGazetteer(BufferedReader reader, double pointRatio, int minPoints, int maxPoints, int maxConsidered)
    throws IOException {
    this.pointRatio = pointRatio;
    this.minPoints = minPoints;
    this.maxPoints = maxPoints;
    this.maxConsidered = maxConsidered;

    this.locations = new ArrayList<Location>();
    this.names = new HashMap<String, List<Location>>();
    this.ipes = new HashMap<String, Integer>();
    this.adms = new HashMap<String, Integer>();
    this.ipePoints = new HashMap<String, List<Coordinate>>();
    this.admPoints = new HashMap<String, List<Coordinate>>();

    this.load(reader);
    this.expandRegions();
  }

  private boolean ignore(String cat, String type) {
    return (cat == "L" || cat == "S" || cat == "U" || cat == "V");
  }

  private boolean store(String cat, String type) {
    return true;
  }

  private void expandRegions() {
    Clusterer clusterer = new KMeans();

    for (String ipe : this.ipes.keySet()) {
      Location location = this.locations.get(this.ipes.get(ipe));
      List<Coordinate> contained = this.ipePoints.get(ipe);

      int k = (int) Math.floor(contained.size() * this.pointRatio);
      if (k < this.minPoints) {
        k = this.minPoints;
      }
      if (k > this.maxPoints) {
        k = this.maxPoints;
      }

      if (contained.size() > this.maxConsidered) {
        Collections.shuffle(contained);
        contained = contained.subList(0, this.maxConsidered);
      }

      List<Coordinate> representatives = clusterer.clusterList(contained, k, SphericalGeometry.g());
      location.setRegion(new PointSetRegion(representatives));
    }
  }

  private int load(BufferedReader reader) {
    int index = 0;
    try {
      for (String line = reader.readLine();
           line != null; line = reader.readLine()) {   
        String[] fields = line.split("\t");
        if (fields.length > 14) {
          String primaryName = fields[1].toLowerCase();
          Set<String> nameSet = new HashSet<String>();
          nameSet.add(primaryName);

          String[] names = fields[3].split(",");
          for (int i = 0; i < names.length; i++) {
            nameSet.add(names[i].toLowerCase());
          }

          String cat = fields[6];
          String type = fields[7];

          if (this.ignore(cat, type)) {
            break;
          }

          String ipe = fields[8];
          String adm = ipe + fields[10];

          double lat = 0.0;
          double lng = 0.0;
          try {      
            lat = Double.parseDouble(fields[4]);
            lng = Double.parseDouble(fields[5]);
          } catch (NumberFormatException e) {
            System.err.format("Invalid coordinates: %s\n", primaryName);
          }
          Coordinate coordinate = Coordinate.fromDegrees(lat, lng);

          int population = 0;
          if (fields[14].length() > 0) {
            try {
              population = Integer.parseInt(fields[14]);
            } catch (NumberFormatException e) {
              System.err.format("Invalid population: %s\n", primaryName);
            }
          }

          if (!this.ipePoints.containsKey(ipe)) {
            this.ipePoints.put(ipe, new ArrayList<Coordinate>());
          }
          this.ipePoints.get(ipe).add(coordinate);

          if (!this.admPoints.containsKey(ipe)) {
            this.admPoints.put(adm, new ArrayList<Coordinate>());
          }
          this.ipePoints.get(ipe).add(coordinate);

          if (type == "PCLI") {
            this.ipes.put(ipe, index);
          } else if (type == "ADM1") {
            this.adms.put(adm, index);
          }

          if (this.store(cat, type)) {
            Region region = new PointRegion(coordinate);
            Location location = new Location(index, primaryName, region, this.getLocationType(cat), population);
            this.locations.add(location);

            for (String name : nameSet) {
              if (!this.names.containsKey(name)) {
                this.names.put(name, new ArrayList<Location>());
              }
              this.names.get(name).add(location);
            }

            index += 1;
          }
        }
      }
      reader.close();
    } catch (IOException e) {
      System.err.format("Error while reading GeoNames file: %s\n", e);
      e.printStackTrace();
    }

    return index;
  }

  private Location.Type getLocationType(String cat) {
    Location.Type type = Location.Type.UNKNOWN;
    if (cat.length() > 0) {
       if (cat.equals("A")) {
         type = Location.Type.STATE;
       } else if (cat.equals("H")) {
         type = Location.Type.WATER;
       } else if (cat.equals("L")) {
         type = Location.Type.PARK;
       } else if (cat.equals("P")) {
         type = Location.Type.CITY;
       } else if (cat.equals("R")) {
         type = Location.Type.TRANSPORT;
       } else if (cat.equals("S")) {
         type = Location.Type.SITE;
       } else if (cat.equals("T")) {
         type = Location.Type.MOUNTAIN;
       } else if (cat.equals("U")) {
         type = Location.Type.UNDERSEA;
       } else if (cat.equals("V")) {
         type = Location.Type.FOREST;
       }
    }
    return type;
  }

  /**
   * Lookup a toponym in the gazetteer, returning null if no candidate list is
   * found.
   */
  public List<Location> lookup(String query) {
    return this.names.get(query.toLowerCase());
  }
}

