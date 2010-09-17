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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.zip.GZIPInputStream;

import opennlp.textgrounder.util.Constants;
import opennlp.textgrounder.topostructs.Coordinate;
import opennlp.textgrounder.topostructs.Location;

public class FilteredGeoNamesReader extends GeoNamesReader {
  public FilteredGeoNamesReader() throws FileNotFoundException, IOException {
    this(new File(Constants.TEXTGROUNDER_DATA + "/gazetteer/allCountries.txt.gz"));
  }

  public FilteredGeoNamesReader(File file) throws FileNotFoundException, IOException {
    this(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))));
  }

  public FilteredGeoNamesReader(BufferedReader reader)
    throws FileNotFoundException, IOException {
    super(reader);
  }

  protected Location parseLine(String line, int currentId) {
    Location location = super.parseLine(line, currentId);
    if (location != null) {
      String type = location.getType();
      if (!type.equals("a") && !type.equals("p")) {
        location = null;
      }
    }
    return location;  
  }
}

