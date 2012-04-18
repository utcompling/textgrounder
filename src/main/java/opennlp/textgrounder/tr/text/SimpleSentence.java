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
package opennlp.textgrounder.tr.text;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import opennlp.textgrounder.tr.util.Span;
import java.io.*;

public class SimpleSentence<A extends Token> extends Sentence<A> implements Serializable {

  private static final long serialVersionUID = 42L;

  private final List<A> tokens;
  private final List<Span<A>> toponymSpans;

  public SimpleSentence(String id, List<A> tokens) {
    this(id, tokens, new ArrayList<Span<A>>());
  }

  public SimpleSentence(String id, List<A> tokens, List<Span<A>> toponymSpans) {
    super(id);
    this.tokens = tokens;
    this.toponymSpans = toponymSpans;
  }

  public Iterator<A> tokens() {
    return this.tokens.iterator();
  }

  public Iterator<Span<A>> toponymSpans() {
    return this.toponymSpans.iterator();
  }
}

