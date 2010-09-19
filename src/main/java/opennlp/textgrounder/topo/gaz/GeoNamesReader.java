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
import java.util.zip.GZIPInputStream;

import opennlp.textgrounder.topo.Coordinate;
import opennlp.textgrounder.topo.DegreeCoordinate;
import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.topo.PointRegion;
import opennlp.textgrounder.topo.Region;
import opennlp.textgrounder.util.Constants;

public class GeoNamesReader extends GazetteerLineReader {
  public GeoNamesReader() throws FileNotFoundException, IOException {
    this(new File(Constants.TEXTGROUNDER_DATA + "/gazetteer/allCountries.txt.gz"));
  }

  public GeoNamesReader(File file) throws FileNotFoundException, IOException {
    this(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))));
  }

  public GeoNamesReader(BufferedReader reader)
    throws FileNotFoundException, IOException {
    super(reader);
  }

  @Override
  protected Location.Type getLocationType(String code) {
    Location.Type type = Location.Type.UNKNOWN;
    if (code.length() > 0) {
       if (code.equals("A")) {
         type = Location.Type.STATE;
       } else if (code.equals("H")) {
         type = Location.Type.WATER;
       } else if (code.equals("L")) {
         type = Location.Type.PARK;
       } else if (code.equals("P")) {
         type = Location.Type.CITY;
       } else if (code.equals("R")) {
         type = Location.Type.TRANSPORT;
       } else if (code.equals("S")) {
         type = Location.Type.SITE;
       } else if (code.equals("T")) {
         type = Location.Type.MOUNTAIN;
       } else if (code.equals("U")) {
         type = Location.Type.UNDERSEA;
       } else if (code.equals("V")) {
         type = Location.Type.FOREST;
       }
    }
    return type;
  }

  protected Location parseLine(String line, int currentId) {
    Location location = null;
    String[] fields = line.split("\t");
    if (fields.length > 14) {
      try {
        String name = fields[1].toLowerCase();
        Location.Type type = this.getLocationType(fields[6], fields[7]);

        double lat = Double.parseDouble(fields[4]);
        double lng = Double.parseDouble(fields[5]);
        Coordinate coordinate = new DegreeCoordinate(lat, lng);
        Region region = new PointRegion(coordinate);

        int population = fields[14].length() == 0 ? 0 : Integer.parseInt(fields[14]);

        location = new Location(currentId, name, region, type, population);
      } catch (NumberFormatException e) {
        System.err.format("Invalid population: %s\n", fields[14]);
      }
    }
    return location;
  }
}

