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
import java.util.NoSuchElementException;

import opennlp.textgrounder.util.Span;

public abstract class Sentence<A extends Token> implements Iterable<A> {
  private final String id;

  protected Sentence(String id) {
    this.id = id;
  }

  public abstract Iterator<A> tokens();

  public Iterator<Span<A>> toponymSpans() {
    return new Iterator<Span<A>>() {      
      public boolean hasNext() {
        return false;
      }

      public Span<A> next() {
        throw new NoSuchElementException();
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public String getId() {
    return this.id;
  }

  public Iterator<A> iterator() {
    return new Iterator<A>() {
      private final Iterator<A> tokens = Sentence.this.tokens();
      private final Iterator<Span<A>> spans = Sentence.this.toponymSpans();
      private int current = 0;
      private Span<A> span = this.spans.hasNext() ? this.spans.next() : null;

      public boolean hasNext() {
        return this.tokens.hasNext();
      }

      public A next() {
        if (this.span != null && this.span.getStart() == this.current) {
          A toponym = span.getItem();
          for (int i = 0; i < this.span.getEnd() - this.span.getStart(); i++) {
            this.tokens.next();
          }
          this.current = this.span.getEnd();
          this.span = this.spans.hasNext() ? this.spans.next() : null;
          return toponym;
        } else {
          this.current++;
          return this.tokens.next();
        }
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}

