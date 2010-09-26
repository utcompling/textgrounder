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

public class StoredCorpus extends StoredItem<Corpus, Document> implements Corpus {
  public StoredCorpus(Corpus wrapped) {
    super(wrapped);
  }

  protected Document wrap(Document document) {
    return new StoredDocument(document);
  }

  private class StoredDocument extends StoredItem<Document, Sentence> implements Document {
    public StoredDocument(Document wrapped) {
      super(wrapped);
    }

    protected Sentence wrap(Sentence sentence) {
      return new StoredSentence(sentence);
    }

    public String getId() {
      return this.getWrapped().getId();
    }
  }

  private class StoredSentence extends StoredItem<Sentence, Token> implements Sentence {
    public StoredSentence(Sentence wrapped) {
      super(wrapped);
    }

    public String getId() {
      return this.getWrapped().getId();
    }
  }
}

/*  private final Corpus wrapped;
  private final List<Document> documents;
  private boolean first;

  public StoredCorpus(Corpus wrapped) {
    this.wrapped = wrapped;
    this.documents = new ArrayList<Document>();
    this.first = true;
  }

  public Iterator<Document> iterator() {
    if (this.first) {
      this.first = false;
      final Iterator<Document> documents = this.wrapped.iterator();
      return new Iterator<Document>() {
        public boolean hasNext() {
          return documents.hasNext();
        }

        public Document next() {
          Document document = new StoredDocument(documents.next());
          StoredCorpus.this.documents.add(document);
          return document;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    } else {
      return this.documents.iterator();
    }
  }

  private class StoredDocument extends Document {
    private final Document wrapped;
    private final List<Sentence> sentences;
    private boolean first;

    private StoredDocument(Document wrapped) {
      super(wrapped.getId());
      this.wrapped = wrapped;
      this.sentences = new ArrayList<Sentence>();
      this.first = true;
    }

    public Iterator<Sentence> iterator() {
      if (this.first) {
        this.first = false;
        final Iterator<Sentence> sentences = this.wrapped.iterator();
        return new Iterator<Sentence>() {
          public boolean hasNext() {
            return sentences.hasNext();
          }

          public Sentence next() {
            Sentence sentence = new StoredSentence(sentences.next());
            StoredDocument.this.sentences.add(sentence);
            return sentence;
          }

          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      } else {
        return this.sentences.iterator();
      }
    }
  }

  private class StoredSentence extends Sentence {
    private final Sentence wrapped;
    private final List<Token> tokens;
    private boolean first;

    private StoredSentence(Sentence wrapped) {
      super(wrapped.getId());
      this.wrapped = wrapped;
      this.sentences = new ArrayList<Token>();
      this.first = true;
    }

    public Iterator<Sentence> iterator() {
      if (this.first) {
        this.first = false;
        final Iterator<Tokens> sentences = this.wrapped.iterator();
        return new Iterator<Sentence>() {
          public boolean hasNext() {
            return sentences.hasNext();
          }

          public Sentence next() {
            Sentence sentence = new StoredSentence(sentences.next());
            StoredDocument.this.sentences.add(sentence);
            return sentence;
          }

          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      } else {
        return this.sentences.iterator();
      }
    }
  }
  @Override
  public void addSource(DocumentSource source) {
    this.wrapped.addSource(source);
  }
}*/

