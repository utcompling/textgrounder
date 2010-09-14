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
package opennlp.textgrounder.gazetteers;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.zip.GZIPInputStream;

import opennlp.textgrounder.util.Constants;
import opennlp.textgrounder.topostructs.Coordinate;
import opennlp.textgrounder.topostructs.Location;

public class GeoNamesReader extends GazetteerReader implements Closeable {
  private BufferedReader reader;
  private String[] current;
  private int currentId;

  public GeoNamesReader() {
    this(new File(Constants.TEXTGROUNDER_DATA + "/gazetteer/allCountries.txt.gz"));
  }

  public GeoNamesReader(File file) {
    try {
      this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
      this.current = this.nextFields();
      this.currentId = 0;
    } catch (IOException e) {      
      System.err.format("Error opening gazetteer file: %s\n", e);
      e.printStackTrace();
      System.exit(1); 
    }
  }

  private String[] toFields(String line) {
    String[] fields = line.split("\t");
    fields[1] = fields[1].toLowerCase();
    fields[6] = fields[6].toLowerCase();
    if (fields[6].equals("a") || fields[6].equals("p")) {
      return fields;
    } else {
      return null;
    }
  }

  private String[] nextFields() {
    String[] fields = null;
    try {
      String line = this.reader.readLine();

      while (line != null && (fields = this.toFields(line)) == null) {
        line = this.reader.readLine();
      }
    } catch (IOException e) {   
      System.err.format("Error while reading gazetteer file: %s\n", e);
      e.printStackTrace();
      fields = null;
    }
    return fields;
  }

  public void close() {
    try {
      this.reader.close();
    } catch (IOException e) {      
      System.err.format("Error closing gazetteer file: %s\n", e);
      e.printStackTrace();
      System.exit(1); 
    }
  }

  public boolean hasNext() {
    return this.current != null;
  }

  public Location next() {
    double lat = Double.parseDouble(this.current[4]);
    double lng = Double.parseDouble(this.current[5]);

    int population = this.current[14].length() == 0 ? 0 : Integer.parseInt(this.current[14]);
    Coordinate coordinate = new Coordinate(lng, lat);

    Location location = new Location(this.currentId++, this.current[1], this.current[6], coordinate, population);

    if (this.currentId % 50000 == 0) System.out.format("At %d\n", this.currentId);

    this.current = this.nextFields();
    return location;
  }
}

