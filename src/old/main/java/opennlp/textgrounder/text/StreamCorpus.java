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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

public class StreamCorpus extends Corpus<Token> {

  private static final long serialVersionUID = 42L;

  private final List<DocumentSource> sources;
  private boolean read;

  StreamCorpus() {
    this.sources = new ArrayList<DocumentSource>();
    this.read = false;
  }

  public Iterator<Document<Token>> iterator() {
    if (this.read) {
      throw new UnsupportedOperationException("Cannot read a stream corpus more than once.");
    } else {
      this.read = true;
      return Iterators.concat(this.sources.iterator());
    }
  }

  public void addSource(DocumentSource source) {
    this.sources.add(source);
  }

  public void close() {
    for (DocumentSource source : this.sources) {
      source.close();
    }
  }
}

