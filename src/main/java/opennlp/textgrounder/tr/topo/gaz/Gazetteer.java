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

import java.util.List;

import opennlp.textgrounder.tr.topo.Location;

/**
 * Represents a mapping from toponym strings to lists of location candidates.
 *
 * @author Travis Brown <travis.brown@mail.utexas.edu>
 */
public interface Gazetteer {
  /**
   * Lookup a toponym in the gazetteer, returning null if no candidate list is
   * found.
   */
  public List<Location> lookup(String query);
}

