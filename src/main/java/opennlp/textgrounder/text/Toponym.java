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

import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.topo.Location;

public class Toponym extends Token implements Iterable<Location> {
  private final int toponymIdx;
  private final List<Location> candidates;
  private final Integer gold;
  private Integer selected;

  Toponym(String form, List<Location> candidates) {
    this(-1, -1, form, candidates, null);
  }

  Toponym(String form, List<Location> candidates, Integer gold) {
    this(-1, -1, form, candidates, gold, null);
  }

  Toponym(String form, List<Location> candidates, Integer gold, Integer selected) {
    this(-1, -1, form, candidates, gold, selected);
  }

  Toponym(int idx, int toponymIdx, String form, List<Location> candidates) {
    this(idx, toponymIdx, form, candidates, null);
  }

  Toponym(int idx, int toponymIdx, String form, List<Location> candidates, Integer gold) {
    this(idx, toponymIdx, form, candidates, gold, null);
  }

  Toponym(int idx, int toponymIdx, String form, List<Location> candidates, Integer gold, Integer selected) {
    super(idx, form);
    this.toponymIdx = toponymIdx;
    this.candidates = candidates;
    this.gold = gold;
    this.selected = selected;
    assert this.gold == null || this.gold < this.candidates.size() : "Invalid candidate index.";
    assert this.selected == null || this.selected < this.candidates.size() : "Invalid candidate index.";
  }

  public int getToponymIdx() {
    if (this.toponymIdx == -1) {
      throw new UnsupportedOperationException("This toponym has no index.");
    } else {
      return this.toponymIdx;
    }
  }

  public boolean hasGold() {
    return this.gold != null;
  }

  public Location getGold() {
    Location location = null;
    if (this.gold != null) {
      location = this.candidates.get(this.gold);
    }
    return location; 
  }

  public int getGoldIdx() {
    return this.gold == null ? -1 : this.gold;
  }

  public boolean hasSelected() {
    return this.selected != null;
  }

  public Location getSelected() {
    Location location = null;
    if (this.selected != null) {
      location = this.candidates.get(this.selected);
    }
    return location;
  }

  public int getSelectedIdx() {
    return this.selected == null ? -1 : this.selected;
  }

  public void setSelected(Integer index) {
    this.selected = index;
  }

  public Iterator<Location> iterator() {
    return this.candidates.iterator();
  }

  @Override
  public boolean isToponym() {
    return true;
  }

  public List<Location> getCandidates() {
    return this.candidates;
  }

  public int getAmbiguity() {
    return this.candidates.size();
  }
}

