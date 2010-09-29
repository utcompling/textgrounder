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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.util.Lexicon;
import opennlp.textgrounder.util.SimpleLexicon;

public class StoredCorpus extends StoredItem<Corpus, Document> implements Corpus {
  private final Lexicon<String> tokenLexicon;
  private final Lexicon<String> toponymLexicon;
  private final List<List<Location>> candidateLists;

  public StoredCorpus(Corpus wrapped) {
    super(wrapped);
    this.tokenLexicon = new SimpleLexicon<String>();
    this.toponymLexicon = new SimpleLexicon<String>();
    this.candidateLists = new ArrayList<List<Location>>();
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

  private class StoredSentence implements Sentence {
    private final String id;
    private final long[] tokens;

    public StoredSentence(Sentence wrapped) {
      this.id = wrapped.getId();

      List<Token> tokenList = new ArrayList<Token>();
      for (Token token : wrapped) {
        tokenList.add(token);
      }

      this.tokens = new long[tokenList.size()];

      for (int i = 0; i < this.tokens.length; i++) {
        Token token = tokenList.get(i);
        int idx = StoredCorpus.this.tokenLexicon.getOrAdd(token.getForm());

        if (token.isToponym()) {
          Toponym toponym = (Toponym) token;
          int toponymIdx = StoredCorpus.this.toponymLexicon.getOrAdd(token.getForm());
          StoredCorpus.this.candidateLists.set(toponymIdx, toponym.getCandidates());

          this.tokens[i] = 1 | ((1 + toponym.getGoldIdx()) << 4)
                             | ((1 + toponym.getSelectedIdx()) << 8)
                             | (toponymIdx << 12) | (idx << 32);
        } else {
          this.tokens[i] = idx << 1;
        }
      }
    }

    public String getId() {
      return this.id;
    }

    public Iterator<Token> iterator() {
      return new Iterator<Token>() {
        private int current = 0;

        public boolean hasNext() {
          return this.current < StoredSentence.this.tokens.length;
        }

        public Token next() {
          long code = StoredSentence.this.tokens[this.current++];
          Token token;

          if ((code & 1) == 1) {
            int idx = (int) code >> 32;
            int toponymIdx  = (int) (code & 0xFFFFF000) >> 12;
            int selectedIdx = ((int) (code & 0x00000F00) >> 8) - 1;
            int goldIdx     = ((int) (code & 0x000000F0) >> 4) - 1;

            String form = StoredCorpus.this.toponymLexicon.atIndex(toponymIdx);
            List<Location> candidates = StoredCorpus.this.candidateLists.get(toponymIdx);
            if (goldIdx > -1) {
              if (selectedIdx > -1) {
                token = new Toponym(idx, toponymIdx, form, candidates, goldIdx, selectedIdx);
              } else {
                token = new Toponym(idx, toponymIdx, form, candidates, goldIdx);
              }
            } else {
              token = new Toponym(idx, toponymIdx, form, candidates);
            }
          } else {
            int idx = (int) code >> 1;
            token = new Token(idx, StoredCorpus.this.tokenLexicon.atIndex(idx));
          }

          return token;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}

