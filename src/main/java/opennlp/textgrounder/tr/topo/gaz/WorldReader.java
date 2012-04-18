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
package opennlp.textgrounder.tr.topo.gaz;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.zip.GZIPInputStream;

import opennlp.textgrounder.tr.topo.Coordinate;
import opennlp.textgrounder.tr.topo.Location;
import opennlp.textgrounder.tr.topo.PointRegion;
import opennlp.textgrounder.tr.topo.Region;

public class WorldReader extends GazetteerLineReader {
  public WorldReader(File file) throws FileNotFoundException, IOException {
    this(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))));
  }

  public WorldReader(BufferedReader reader)
    throws FileNotFoundException, IOException {
    super(reader);
  }

  private double convertNumber(String number) {
    boolean negative = number.charAt(0) == '-';
    number = "00" + (negative ? number.substring(1) : number);
    int split = number.length() - 2;
    number = number.substring(0, split) + "." + number.substring(split);
    return Double.parseDouble(number) * (negative ? -1 : 1);
  }

  protected Location parseLine(String line, int currentId) {
    Location location = null;
    String[] fields = line.split("\t");
    if (fields.length > 7 && fields[6].length() > 0 && fields[7].length() > 0 &&
        !(fields[6].equals("0") && fields[7].equals("9999"))) {
      String name = fields[1].toLowerCase();
      Location.Type type = this.getLocationType(fields[4].toLowerCase());

      double lat = this.convertNumber(fields[6].trim());
      double lng = this.convertNumber(fields[7].trim());
      Coordinate coordinate = Coordinate.fromDegrees(lat, lng);
      Region region = new PointRegion(coordinate);

      int population = fields[5].trim().length() > 0 ? Integer.parseInt(fields[5]) : 0;

      /*String container = null;
      if (fields.length > 10 && fields[10].trim().length() > 0) {
        container = fields[10].trim().toLowerCase();
      }*/

      location = new Location(currentId, name, region, type, population);
    }
    return location;  
  }
}

