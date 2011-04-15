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

import java.io.*;
import java.util.*;

import opennlp.textgrounder.topo.Coordinate;
import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.topo.PointRegion;
import opennlp.textgrounder.topo.PointSetRegion;
import opennlp.textgrounder.topo.Region;
import opennlp.textgrounder.topo.SphericalGeometry;
import opennlp.textgrounder.util.cluster.Clusterer;
import opennlp.textgrounder.util.cluster.KMeans;

public class GeoNamesGazetteer implements Gazetteer, Serializable {
  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
private final boolean expandRegions;
  private final double pointRatio;
  private final int minPoints;
  private final int maxPoints;
  private final int maxConsidered;

  private final List<Location> locations;
  private final Map<String, List<Location>> names;
  private final Map<String, Integer> ipes;
  //private final Map<String, Integer> adms;
  private Map<String, List<Coordinate>> ipePoints; // made mutable so can assign to null when done for faster GC
  //private final Map<String, List<Coordinate>> admPoints;

  public GeoNamesGazetteer(BufferedReader reader) throws IOException {
    this(reader, true, 0.005);
  }

  public GeoNamesGazetteer(BufferedReader reader, boolean expandRegions) throws IOException {
    this(reader, expandRegions, 0.005);
  }

  public GeoNamesGazetteer(BufferedReader reader, boolean expandRegions, int kPoints)
    throws IOException {
    this(reader, expandRegions, 1.0, kPoints, kPoints);
  }

  public GeoNamesGazetteer(BufferedReader reader, boolean expandRegions, double pointRatio)
    throws IOException {
    this(reader, expandRegions, pointRatio, 5, 30);
  }

  public GeoNamesGazetteer(BufferedReader reader, boolean expandRegions, double pointRatio, int minPoints, int maxPoints)
    throws IOException {
    this(reader, expandRegions, pointRatio, minPoints, maxPoints, 2000);
  }

  public GeoNamesGazetteer(BufferedReader reader, boolean expandRegions, double pointRatio, int minPoints, int maxPoints, int maxConsidered)
    throws IOException {
    this.expandRegions = expandRegions;
    this.pointRatio = pointRatio;
    this.minPoints = minPoints;
    this.maxPoints = maxPoints;
    this.maxConsidered = maxConsidered;

    this.locations = new ArrayList<Location>();
    this.names = new HashMap<String, List<Location>>();
    this.ipes = new HashMap<String, Integer>();
    //this.adms = new HashMap<String, Integer>();
    this.ipePoints = new HashMap<String, List<Coordinate>>();
    //this.admPoints = new HashMap<String, List<Coordinate>>();

    this.load(reader);
    if (this.expandRegions) {
      this.expandIPE();
      //this.expandADM();
    }
  }

  private boolean ignore(String cat, String type) {
    return (cat.equals("H") || cat.equals("L") || cat.equals("S") || cat.equals("U") || cat.equals("V"));
  }

  private boolean store(String cat, String type) {
    return true;
  }

  private void expandIPE() {
    Clusterer clusterer = new KMeans();

    System.out.println("Selecting points for " + this.ipes.size() + " independent political entities.");
    for (String ipe : this.ipes.keySet()) {
      Location location = this.locations.get(this.ipes.get(ipe));
      List<Coordinate> contained = this.ipePoints.get(ipe);// ALL points in e.g. USA

      int k = (int) Math.floor(contained.size() * this.pointRatio);
      if (k < this.minPoints) {
        k = this.minPoints;
      }
      if (k > this.maxPoints) {
        k = this.maxPoints;
      }

      //System.err.format("Clustering: %d points for %s.\n", k, location.getName());

      if (contained.size() > this.maxConsidered) {
        Collections.shuffle(contained);
        contained = contained.subList(0, this.maxConsidered);
      }

      if (contained.size() > 0) {
        List<Coordinate> representatives = clusterer.clusterList(contained, k, SphericalGeometry.g());
        location.setRegion(new PointSetRegion(representatives));
        contained.clear();
        contained = null;
      }
      this.ipePoints.get(ipe).clear();
    }
    this.ipePoints.clear();
    this.ipePoints = null;
  }

  /*private void expandADM() {
    Clusterer clusterer = new KMeans();

    System.out.println("Selecting points for " + this.adms.size() + " administrative regions.");
    for (String adm : this.adms.keySet()) {
      Location location = this.locations.get(this.adms.get(adm));
      List<Coordinate> contained = this.admPoints.get(adm);

      if (contained != null) {
        int k = (int) Math.floor(contained.size() * this.pointRatio);
        if (k < this.minPoints) {
          k = this.minPoints;
        }
        if (k > this.maxPoints) {
          k = this.maxPoints;
        }

        //System.err.format("Clustering: %d points for %s.\n", k, location.getName());

        if (contained.size() > this.maxConsidered) {
          Collections.shuffle(contained);
          contained = contained.subList(0, this.maxConsidered);
        }

        if (contained.size() > 0) {
          List<Coordinate> representatives = clusterer.clusterList(contained, k, SphericalGeometry.g());
          location.setRegion(new PointSetRegion(representatives));

          /*for (Coordinate c : representatives) {
            System.out.println("<Placemark><Point><coordinates>" +
                               c.getLngDegrees() + "," + c.getLatDegrees() +
                               "</coordinates></Point></Placemark>");
          }*SLASH
        }
      }
    }
  }*/

  private String standardize(String name) {
    return name.toLowerCase().replace("’", "'");
  }

  private int load(BufferedReader reader) {
    int index = 0;
    int count = 0;
    try {
      System.out.print("[");
      for (String line = reader.readLine();
           line != null; line = reader.readLine()) {   
        String[] fields = line.split("\t");
        if (fields.length > 14) {
          String primaryName = fields[1];
          count++;
          if(count % 750000 == 0) {
            System.out.print(".");
          }
          Set<String> nameSet = new HashSet<String>();
          nameSet.add(this.standardize(primaryName));

          String[] names = fields[3].split(",");
          for (int i = 0; i < names.length; i++) {
            nameSet.add(this.standardize(names[i]));
          }

          String cat = fields[6];
          String type = fields[7];

          if (this.ignore(cat, type)) {
            continue;
          }

          String ipe = fields[8];
          String adm = ipe + fields[10];

          String admin1code = ipe + "." + fields[10];

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

          /*if (!this.admPoints.containsKey(adm)) {
            this.admPoints.put(adm, new ArrayList<Coordinate>());
          }
          this.admPoints.get(adm).add(coordinate);*/

          if (type.equals("PCLI")) {
            this.ipes.put(ipe, index);
          } /*else if (type.equals("ADM1")) {
            this.adms.put(adm, index);
          }*/

          if (this.store(cat, type)) {
            Region region = new PointRegion(coordinate);
            Location location = new Location(index, primaryName, region, this.getLocationType(cat), population, admin1code);
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
      System.out.println("]");
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
  
  public Set<String> getUniqueLocationNameSet(){
	  return names.keySet();
  }
}
