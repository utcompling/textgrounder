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

public class Document implements Iterable<Sentence> {
  private final String id;
  private final Iterable<Sentence> sentences;

  public Document(String id, Iterable<Sentence> sentences) {
    this.id = id;
    this.sentences = sentences;
  }

  public String getId() {
    return this.id;
  }

  public Iterator<Sentence> iterator() {
    return this.sentences.iterator();
  }
}

