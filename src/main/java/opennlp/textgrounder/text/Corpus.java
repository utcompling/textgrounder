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

import opennlp.textgrounder.util.Lexicon;

public abstract class Corpus<A extends Token> implements Iterable<Document<A>> {
  public abstract void addSource(DocumentSource source);
  public abstract void close();

  public static Corpus<Token> createStreamCorpus() {
    return new StreamCorpus();
  }

  public static StoredCorpus createStoredCorpus() {
    return new CompactCorpus(Corpus.createStreamCorpus());
  }

  public DocumentSource asSource() {
    final Iterator<Document<A>> iterator = this.iterator();

    return new DocumentSource() {
      public boolean hasNext() {
        return iterator.hasNext();
      }

      public Document<Token> next() {
        return (Document<Token>) iterator.next();
      }
    };
  }
}

