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

import com.google.common.collect.Iterators;
//import com.google.inject.Inject;

public class StreamCorpus implements Corpus {
  private final List<DocumentSource> sources;

  public StreamCorpus() {
    this.sources = new ArrayList<DocumentSource>();
  }

  public Iterator<Document> iterator() {
    return Iterators.concat(this.sources.iterator());
  }

  public void addSource(DocumentSource source) {
    this.sources.add(source);
  }
}

