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

import java.util.Iterator;
import opennlp.textgrounder.tr.topo.Location;

public abstract class GazetteerReader implements Iterable<Location>,
                                                 Iterator<Location> {
  public abstract void close();

  protected Location.Type getLocationType(String code) {
    return Location.Type.UNKNOWN;
  }

  protected Location.Type getLocationType(String code, String fine) {
    return this.getLocationType(code);
  }

  public Iterator<Location> iterator() {
    return this;
  }

  public void remove() {
    throw new UnsupportedOperationException("Cannot remove location from gazetteer.");
  }
}

