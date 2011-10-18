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
import java.io.*;

public class SimpleToponym extends SimpleToken implements Toponym, Serializable {

  private static final long serialVersionUID = 42L;

  private List<Location> candidates;
  private int goldIdx;
  private int selectedIdx;

  public SimpleToponym(String form, List<Location> candidates) {
    this(form, candidates, -1);
  }

  public SimpleToponym(String form, List<Location> candidates, int goldIdx) {
    this(form, candidates, goldIdx, -1);
  }

  public SimpleToponym(String form, List<Location> candidates, int goldIdx, int selectedIdx) {
    super(form);
    this.candidates = candidates;
    this.goldIdx = goldIdx;
    this.selectedIdx = selectedIdx;
  }

  public boolean hasGold() {
    return this.goldIdx > -1;
  }

  public Location getGold() {
    return this.candidates.get(this.goldIdx);
  }

  public int getGoldIdx() {
    return this.goldIdx;
  }

    public void setGoldIdx(int idx) {
        this.goldIdx = idx;
    }

  public boolean hasSelected() {
    return this.selectedIdx > -1;
  }

  public Location getSelected() {
    return this.candidates.get(this.selectedIdx);
  }

  public int getSelectedIdx() {
    return this.selectedIdx;
  }

  public void setSelectedIdx(int idx) {
    this.selectedIdx = idx;
  }

  public int getAmbiguity() {
    return this.candidates.size();
  }

  public List<Location> getCandidates() {
    return this.candidates;
  }

  public void setCandidates(List<Location> candidates) {
    this.candidates = candidates;
  }

  public List<Token> getTokens() {
    throw new UnsupportedOperationException("Can't currently get tokens.");
  }

  public Iterator<Location> iterator() {
    return this.candidates.iterator();
  }

  @Override
  public boolean isToponym() {
    return true;
  }
}

