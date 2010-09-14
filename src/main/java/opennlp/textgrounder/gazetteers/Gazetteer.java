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

import java.util.List;
import opennlp.textgrounder.topostructs.Location;

public abstract class Gazetteer {
  public abstract void add(String name, Location location);
  public abstract List<Location> lookup(String query);

  public boolean contains(String query) {
    return !this.lookup(query).isEmpty();
  }

  public int load(GazetteerReader reader) {
    int count = 0;
    for (Location location : reader) {
      count++;
      this.add(location.getName(), location);
    }
    reader.close();
    this.finishLoading();
    return count;
  }

  public void finishLoading() {}
  public void close() {}
}

