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
package opennlp.textgrounder.old.gazetteers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import opennlp.textgrounder.old.topostructs.Location;

public abstract class GazetteerLineReader extends GazetteerFileReader {
  private Location current;
  private int currentId;

  protected GazetteerLineReader(BufferedReader reader)
    throws FileNotFoundException, IOException {
    super(reader);
    this.current = this.nextLocation();
    this.currentId = 1;
  }

  protected abstract Location parseLine(String line, int currentId);

  private Location nextLocation() {
    Location location = null;
    for (String line = this.readLine(); line != null; line = this.readLine()) {
      location = this.parseLine(line, this.currentId);
      if (location != null) break;
    }
    this.currentId++;
    if (this.currentId % 50000 == 0) { System.out.format("At location id: %d.\n", this.currentId); }
    return location;
  }

  public boolean hasNext() {
    return this.current != null;
  }

  public Location next() {
    Location location = this.current;
    this.current = this.nextLocation();
    return location;
  }
}

