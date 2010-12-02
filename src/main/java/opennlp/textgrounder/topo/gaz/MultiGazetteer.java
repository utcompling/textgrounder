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
import java.util.List;
import opennlp.textgrounder.topo.Location;

public class MultiGazetteer implements Gazetteer {
  private final List<Gazetteer> gazetteers;

  public MultiGazetteer(List<Gazetteer> gazetteers) {
    this.gazetteers = gazetteers;
  }

  public List<Location> lookup(String query) {
    for (Gazetteer gazetteer : this.gazetteers) {
      List<Location> candidates = gazetteer.lookup(query);
      if (candidates != null) {
        return candidates;
      }
    }
    return null;
  }
}

