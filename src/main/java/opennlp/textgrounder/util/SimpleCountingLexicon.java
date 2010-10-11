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

public class SimpleCountingLexicon<A extends Serializable>
  extends SimpleLexicon<A> implements CountingLexicon<A> {
  private ArrayList<Integer> counts;

  public SimpleCountingLexicon(int capacity) {
    super(capacity);
    this.counts = new ArrayList<Integer>(capacity);
  }

  public SimpleCountingLexicon() {
    this(2048);
  }

  public int getOrAdd(A entry) {
    Integer index = this.map.get(entry);
    if (index == null) {
      if (this.growing) {
        index = this.entries.size();
        this.map.put(entry, index);
        this.entries.add(entry);
        this.counts.add(1);
      } else {
        throw new UnsupportedOperationException("Cannot add to a non-growing lexicon.");
      }
    } else {
      this.counts.set(index, this.counts.get(index) + 1);
    }

    return index;
  }

  public int count(A entry) {
    return this.counts.get(this.map.get(entry));
  }

  public int countAtIndex(int index) {
    return this.counts.get(index);
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeBoolean(this.growing);
    out.writeObject(this.entries);
    out.writeObject(this.counts);
  }

  private void readObject(ObjectInputStream in)
    throws IOException, ClassNotFoundException {
    this.growing = in.readBoolean();
    this.entries = (ArrayList<A>) in.readObject();
    this.counts = (ArrayList<Integer>) in.readObject();
    this.map = new HashMap<A, Integer>(this.entries.size());

    for (int i = 0; i < this.entries.size(); i++) {
      this.map.put(this.entries.get(i), i);
    }
  }
}

