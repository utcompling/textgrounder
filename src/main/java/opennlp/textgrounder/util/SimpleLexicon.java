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
package opennlp.textgrounder.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleLexicon<A extends Serializable> implements Lexicon<A> {
  protected Map<A, Integer> map;
  protected ArrayList<A> entries;
  protected boolean growing;

  public SimpleLexicon(int capacity) {
    this.map = new HashMap<A, Integer>(capacity, 0.5F);
    this.entries = new ArrayList<A>(capacity);
    this.growing = true;
  }

  public SimpleLexicon() {
    this(2048);
  }

  public boolean contains(A entry) {
    return this.map.containsKey(entry);
  }

  public int get(A entry) {
    Integer index = this.map.get(entry);
    return (index == null) ? -1 : index;
  }

  public int getOrAdd(A entry) {
    Integer index = this.map.get(entry);
    if (index == null) {
      if (this.growing) {
        index = this.entries.size();
        this.map.put(entry, index);
        this.entries.add(entry);
      } else {
        throw new UnsupportedOperationException("Cannot add to a non-growing lexicon.");
      }
    }
    return index;
  }

  public A atIndex(int index) {
    return this.entries.get(index);
  }

  public int size() {
    return this.entries.size();
  }

  public boolean isGrowing() {
    return this.growing;
  }

  public void stopGrowing() {
    if (this.growing) {
      this.entries.trimToSize();
      this.growing = false;
    }
  }

  public void startGrowing() {
    this.growing = true;
  }

  public List<Integer> concatenate(Lexicon<A> other) {
    List<Integer> conversions = new ArrayList<Integer>(other.size());
    for (A entry : other) {
      conversions.add(this.getOrAdd(entry));
    }
    return conversions;
  }

  public Iterator<A> iterator() {
    return this.entries.iterator();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeBoolean(this.growing);
    out.writeObject(this.entries);
  }

  private void readObject(ObjectInputStream in)
    throws IOException, ClassNotFoundException {
    this.growing = in.readBoolean();
    this.entries = (ArrayList<A>) in.readObject();
    this.map = new HashMap<A, Integer>(this.entries.size());

    for (int i = 0; i < this.entries.size(); i++) {
      this.map.put(this.entries.get(i), i);
    }
  }
}

