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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StoredItem<A extends Iterable<B>, B> implements Iterable<B> {
  private final A wrapped;
  private final List<B> items;
  private boolean first;

  public StoredItem(A wrapped) {
    this.wrapped = wrapped;
    this.items = new ArrayList<B>();
    this.first = true;
  }

  protected B wrap(B item) {
    return item;
  }

  public Iterator<B> iterator() {
    if (this.first) {
      this.first = false;
      return new StoredIterator(this.wrapped.iterator());
    } else {
      return this.items.iterator();
    }
  }

  public A getWrapped() {
    return this.wrapped;
  }

  private class StoredIterator implements Iterator<B> {
    private final Iterator<B> iterator;

    protected StoredIterator(Iterator<B> iterator) {
      this.iterator = iterator;
    }

    public boolean hasNext() {
      return this.iterator.hasNext();
    }

    public B next() {
      B item = StoredItem.this.wrap(this.iterator.next());
      StoredItem.this.items.add(item);
      return item;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

