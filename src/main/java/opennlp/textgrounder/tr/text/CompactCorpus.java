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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;

import opennlp.textgrounder.tr.topo.*;
import opennlp.textgrounder.tr.util.*;
import opennlp.textgrounder.tr.app.*;

public class CompactCorpus extends StoredCorpus implements Serializable {

  private static final long serialVersionUID = 42L;

  private Corpus<Token> wrapped;

  private final CountingLexicon<String> tokenLexicon;
  private final CountingLexicon<String> toponymLexicon;

  private final CountingLexicon<String> tokenOrigLexicon;
  private final CountingLexicon<String> toponymOrigLexicon;

  private int[] tokenOrigMap;
  private int[] toponymOrigMap;

  private int maxToponymAmbiguity;
  private double avgToponymAmbiguity = 0.0;
  private int tokenCount = 0;
  private int toponymTokenCount = 0;

  private final ArrayList<Document<StoredToken>> documents;
  private final ArrayList<List<Location>> candidateLists;
  
  CompactCorpus(Corpus<Token> wrapped) {
    this.wrapped = wrapped;

    this.tokenLexicon = new SimpleCountingLexicon<String>();
    this.toponymLexicon = new SimpleCountingLexicon<String>();
    this.tokenOrigLexicon = new SimpleCountingLexicon<String>();
    this.toponymOrigLexicon = new SimpleCountingLexicon<String>();
    this.maxToponymAmbiguity = 0;

    this.documents = new ArrayList<Document<StoredToken>>();
    this.candidateLists = new ArrayList<List<Location>>();
  }

  public int getDocumentCount() {
    return this.documents.size();
  }

  public int getTokenTypeCount() {
    return this.tokenLexicon.size();
  }

  public int getTokenOrigTypeCount() {
    return this.tokenOrigLexicon.size();
  }

  public int getToponymTypeCount() {
    return this.toponymLexicon.size();
  }

  public int getToponymOrigTypeCount() {
    return this.toponymOrigLexicon.size();
  }

  public int getMaxToponymAmbiguity() {
    return this.maxToponymAmbiguity;
  }

  public double getAvgToponymAmbiguity() {
    return this.avgToponymAmbiguity;
  }

  public int getTokenCount() {
    return this.tokenCount;
  }

  public int getToponymTokenCount() {
    return this.toponymTokenCount;
  }

  public void load() {
    for (Document<Token> document : wrapped) {
      ArrayList<Sentence<StoredToken>> sentences = new ArrayList<Sentence<StoredToken>>();

      for (Sentence<Token> sentence : document) {
        List<Token> tokens = sentence.getTokens();
        int[] tokenIdxs = new int[tokens.size()];
        this.tokenCount += tokens.size();

        for (int i = 0; i < tokenIdxs.length; i++) {
          Token token = tokens.get(i);
          tokenIdxs[i] = this.tokenOrigLexicon.getOrAdd(token.getOrigForm());
          this.tokenLexicon.getOrAdd(token.getForm());
        }

        StoredSentence stored = new StoredSentence(sentence.getId(), tokenIdxs);

        for (Iterator<Span<Token>> it = sentence.toponymSpans(); it.hasNext(); ) {
          Span<Token> span = it.next();
          Toponym toponym = (Toponym) span.getItem();

          this.toponymTokenCount++;

          this.avgToponymAmbiguity += toponym.getAmbiguity();

          if (toponym.getAmbiguity() > this.maxToponymAmbiguity) {
            this.maxToponymAmbiguity = toponym.getAmbiguity();
          }

          int idx = this.toponymOrigLexicon.getOrAdd(toponym.getOrigForm());
          this.toponymLexicon.getOrAdd(toponym.getForm());

          if (toponym.hasGold()) {
            int goldIdx = toponym.getGoldIdx();
            if (toponym.hasSelected()) {
              int selectedIdx = toponym.getSelectedIdx();
              stored.addToponym(span.getStart(), span.getEnd(), idx, goldIdx, selectedIdx);
            } else {
              stored.addToponym(span.getStart(), span.getEnd(), idx, goldIdx);
            }
          } else {
              if(toponym.hasSelected()) {
                  int selectedIdx = toponym.getSelectedIdx();
                  stored.addToponym(span.getStart(), span.getEnd(), idx, -1, selectedIdx);
              }
              else {
                  stored.addToponym(span.getStart(), span.getEnd(), idx);
              }
          }

          if (this.candidateLists.size() <= idx) {
            this.candidateLists.add(toponym.getCandidates());
          } else {
            this.candidateLists.set(idx, toponym.getCandidates());
          }
        }

        stored.compact();
        sentences.add(stored);
      }

      sentences.trimToSize();
      if(this.getFormat() == BaseApp.CORPUS_FORMAT.GEOTEXT) {
          this.documents.add(new StoredDocument(document.getId(), sentences,
                                                document.getTimestamp(),
                                                document.getGoldCoord(), document.getSystemCoord(), document.getSection()));
      }
      else
          this.documents.add(new StoredDocument(document.getId(), sentences));
    }

    this.avgToponymAmbiguity /= this.toponymTokenCount;

    this.tokenOrigMap = new int[this.tokenOrigLexicon.size()];
    this.toponymOrigMap = new int[this.toponymOrigLexicon.size()];

    int i = 0;
    for (String entry : this.tokenOrigLexicon) {
      this.tokenOrigMap[i] = this.tokenLexicon.get(entry.toLowerCase());
      i++;
    }

    i = 0;
    for (String entry : this.toponymOrigLexicon) {
      this.toponymOrigMap[i] = this.toponymLexicon.get(entry.toLowerCase());
      i++;
    }
    
    this.wrapped.close();
    this.wrapped = null;

    this.removeNaNs();
  }

  private void removeNaNs() {
      for(List<Location> candidates : candidateLists) {
          for(Location loc : candidates) {
              List<Coordinate> reps = loc.getRegion().getRepresentatives();
              int prevSize = reps.size();
              Coordinate.removeNaNs(reps);
              if(reps.size() < prevSize)
                  loc.getRegion().setCenter(Coordinate.centroid(reps));
          }
      }
  }

  public void addSource(DocumentSource source) {
    if (this.wrapped == null) {
      throw new UnsupportedOperationException("Cannot add a source to a stored corpus after it has been loaded.");
    } else {
      this.wrapped.addSource(source);
    }
  }

  public Iterator<Document<StoredToken>> iterator() {
    if (this.wrapped != null) {
      this.load();
    }

    return this.documents.iterator();
  }

  private class StoredDocument extends Document<StoredToken> implements Serializable {

    private static final long serialVersionUID = 42L;

    private final List<Sentence<StoredToken>> sentences;

    private StoredDocument(String id, List<Sentence<StoredToken>> sentences) {
      super(id);
      this.sentences = sentences;
    }

    private StoredDocument(String id, List<Sentence<StoredToken>> sentences, String timestamp, Coordinate goldCoord, Coordinate systemCoord) {
        this(id, sentences, timestamp, goldCoord);
        this.systemCoord = systemCoord;
    }

    private StoredDocument(String id, List<Sentence<StoredToken>> sentences, String timestamp, double goldLat, double goldLon) {
        this(id, sentences);
        this.timestamp = timestamp;
        this.goldCoord = Coordinate.fromDegrees(goldLat, goldLon);
    }

    private StoredDocument(String id, List<Sentence<StoredToken>> sentences, String timestamp, Coordinate goldCoord) {
        this(id, sentences);
        this.timestamp = timestamp;
        this.goldCoord = goldCoord;
    }

    private StoredDocument(String id, List<Sentence<StoredToken>> sentences, String timestamp, Coordinate goldCoord, Coordinate systemCoord, Enum<Document.SECTION> section) {
        this(id, sentences, timestamp, goldCoord, systemCoord);
        this.section = section;
    }

    public Iterator<Sentence<StoredToken>> iterator() {
      return this.sentences.iterator();
    }
  }

  private class StoredSentence extends Sentence<StoredToken> implements Serializable {

    private static final long serialVersionUID = 42L;

    private final int[] tokens;
    private final ArrayList<Span<StoredToken>> toponymSpans;

    private StoredSentence(String id, int[] tokens) {
      super(id);
      this.tokens = tokens;
      this.toponymSpans = new ArrayList<Span<StoredToken>>();
    }

    private void addToponym(int start, int end, int toponymIdx) {
      this.toponymSpans.add(new Span<StoredToken>(start, end, new CompactToponym(toponymIdx)));
    }

    private void addToponym(int start, int end, int toponymIdx, int goldIdx) {
      this.toponymSpans.add(new Span<StoredToken>(start, end, new CompactToponym(toponymIdx, goldIdx)));
    }

    private void addToponym(int start, int end, int toponymIdx, int goldIdx, int selectedIdx) {
      this.toponymSpans.add(new Span<StoredToken>(start, end, new CompactToponym(toponymIdx, goldIdx, selectedIdx)));
    }

    private void compact() {
      this.toponymSpans.trimToSize();
    }

    private class CompactToponym implements StoredToponym {

      private static final long serialVersionUID = 42L;

      private final int idx;
      private int goldIdx;
      private int selectedIdx;

      private CompactToponym(int idx) {
        this(idx, -1);
      }

      private CompactToponym(int idx, int goldIdx) {
        this(idx, goldIdx, -1);
      }

      private CompactToponym(int idx, int goldIdx, int selectedIdx) {
        this.idx = idx;
        this.goldIdx = goldIdx;
        this.selectedIdx = selectedIdx;
      }

      public String getForm() {
        return CompactCorpus.this.toponymLexicon.atIndex(CompactCorpus.this.toponymOrigMap[this.idx]);
      }

      public String getOrigForm() {
        return CompactCorpus.this.toponymOrigLexicon.atIndex(this.idx);
      }

      public boolean isToponym() {
        return true;
      }

      public boolean hasGold() { return this.goldIdx > -1; }
      public Location getGold() {
        if (this.goldIdx == -1) {
          return null;
        } else {
          return CompactCorpus.this.candidateLists.get(this.idx).get(this.goldIdx);
        }
      }
      public int getGoldIdx() { return this.goldIdx; }
      public void setGoldIdx(int idx) { this.goldIdx = idx; }

      public boolean hasSelected() { return this.selectedIdx > -1; }
      public Location getSelected() {
        if (this.selectedIdx == -1) {
          return null;
        } else {
          return CompactCorpus.this.candidateLists.get(this.idx).get(this.selectedIdx);
        }
      }

      public int getSelectedIdx() { return this.selectedIdx; }
      public void setSelectedIdx(int idx) { this.selectedIdx = idx; }

      public int getAmbiguity() { return CompactCorpus.this.candidateLists.get(this.idx).size(); }
      public List<Location> getCandidates() { return CompactCorpus.this.candidateLists.get(this.idx); }
      public void setCandidates(List<Location> candidates) { CompactCorpus.this.candidateLists.set(this.idx, candidates); }
      public Iterator<Location> iterator() { return CompactCorpus.this.candidateLists.get(this.idx).iterator(); }

      public List<Token> getTokens() { throw new UnsupportedOperationException(); }

      public int getIdx() {
        return CompactCorpus.this.toponymOrigMap[this.idx];
      }

      public int getOrigIdx() {
        return this.idx;
      }

      public int getTypeCount() {
        return CompactCorpus.this.toponymLexicon.countAtIndex(CompactCorpus.this.toponymOrigMap[this.idx]);
      }

      public int getOrigTypeCount() {
        return CompactCorpus.this.toponymOrigLexicon.countAtIndex(idx);
      }

      @Override
      public boolean equals(Object other) {
        return other != null && 
               other.getClass() == this.getClass() &&
               ((StoredToponym) other).getIdx() == this.getIdx();
      }
    }

    public Iterator<StoredToken> tokens() {
      return new Iterator<StoredToken>() {
        private int current = 0;

        public boolean hasNext() {
          return this.current < StoredSentence.this.tokens.length;
        }

        public StoredToken next() {
          final int current = this.current++;
          final int idx = StoredSentence.this.tokens[current];
          return new StoredToken() {

            private static final long serialVersionUID = 42L;

            public String getForm() {
              return CompactCorpus.this.tokenLexicon.atIndex(CompactCorpus.this.tokenOrigMap[idx]);
            }

            public String getOrigForm() {
              return CompactCorpus.this.tokenOrigLexicon.atIndex(idx);
            }

            public boolean isToponym() {
              return false;
            }

            public int getIdx() {
              return CompactCorpus.this.tokenOrigMap[idx];
            }

            public int getOrigIdx() {
              return idx;
            }

            public int getTypeCount() {
              return CompactCorpus.this.tokenLexicon.countAtIndex(CompactCorpus.this.tokenOrigMap[idx]);
            }

            public int getOrigTypeCount() {
              return CompactCorpus.this.tokenOrigLexicon.countAtIndex(idx);
            }
          };
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    public Iterator<Span<StoredToken>> toponymSpans() {
      return this.toponymSpans.iterator();
    }
  }

  public void close() {
    if (this.wrapped != null) {
      this.wrapped.close();
    }
  }
}

