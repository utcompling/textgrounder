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
package opennlp.textgrounder.text;

import java.util.List;
import java.io.*;

import opennlp.textgrounder.topo.Location;

public interface Toponym extends Token, Iterable<Location>, Serializable {
  public boolean hasGold();
  public Location getGold();
  public int getGoldIdx();

  public boolean hasSelected();
  public Location getSelected();
  public int getSelectedIdx();

  public void setSelectedIdx(int idx);

  public int getAmbiguity();
  public List<Location> getCandidates();

  public List<Token> getTokens();
}

