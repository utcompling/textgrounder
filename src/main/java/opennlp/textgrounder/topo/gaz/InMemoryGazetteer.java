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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import opennlp.textgrounder.topo.Location;

public class InMemoryGazetteer extends LoadableGazetteer {
  private final Map<String, List<Location>> map;

  public InMemoryGazetteer() {
    this.map = new HashMap<String, List<Location>>();
  }

  public void add(String name, Location location) {
    name = name.toLowerCase();
    List<Location> locations = this.map.get(name);
    if (locations == null) {
      locations = new ArrayList<Location>();
    }
    locations.add(location);
    this.map.put(name, locations);
  }

  public List<Location> lookup(String query) {
    query = query.toLowerCase();
    List<Location> locations = this.map.get(query);
    if (locations == null) {
      locations = new ArrayList<Location>(0);
    }
    return locations;
  }
}

