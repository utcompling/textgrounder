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
package opennlp.textgrounder.tr.text.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.tr.text.Document;
import opennlp.textgrounder.tr.text.Sentence;
import opennlp.textgrounder.tr.text.SimpleToken;
import opennlp.textgrounder.tr.text.Token;
//import opennlp.textgrounder.tr.text.ner.NamedEntityRecognizer;
import opennlp.textgrounder.tr.text.prep.Tokenizer;
import opennlp.textgrounder.tr.text.prep.SentenceDivider;
//import opennlp.textgrounder.tr.topo.gaz.Gazetteer;

public class PlainTextSource extends TextSource {
  private final SentenceDivider divider;
  private final Tokenizer tokenizer;
  //private final NamedEntityRecognizer recognizer;
  //private final Gazetteer gazetteer;

  private int number;
  private String current;
  private int currentIdx;
  private int parasPerDocument;
  private String docId;

  public PlainTextSource(BufferedReader reader, SentenceDivider divider, Tokenizer tokenizer, String id)
    throws IOException {
      this(reader, divider, tokenizer, id, -1);
  }

    public PlainTextSource(BufferedReader reader, SentenceDivider divider, Tokenizer tokenizer,
                           String id, int parasPerDocument)
    throws IOException {
    super(reader);
    this.divider = divider;
    this.tokenizer = tokenizer;
    //this.recognizer = recognizer;
    //this.gazetteer = gazetteer;
    this.current = this.readLine();
    this.number = 0;
    this.currentIdx = 0;
    this.parasPerDocument = parasPerDocument;
    this.docId = id;
  }

  public boolean hasNext() {
    return this.current != null;
  }

  public Document<Token> next() {
    return new Document<Token>(this.docId) {
      private static final long serialVersionUID = 42L;
      public Iterator<Sentence<Token>> iterator() {
        final List<List<Token>> sentences = new ArrayList<List<Token>>();
        while (PlainTextSource.this.current != null &&
               (PlainTextSource.this.parasPerDocument == -1 ||
                (PlainTextSource.this.current.length() > 0 &&
                 PlainTextSource.this.number < PlainTextSource.this.parasPerDocument))) {
          for (String sentence : PlainTextSource.this.divider.divide(PlainTextSource.this.current)) {
            List<Token> tokens = new ArrayList<Token>();
            for (String token : PlainTextSource.this.tokenizer.tokenize(sentence)) {
              tokens.add(new SimpleToken(token));
            }
            sentences.add(tokens);
          }
          PlainTextSource.this.number++;
          PlainTextSource.this.current = PlainTextSource.this.readLine();
        }
        PlainTextSource.this.number = 0;
        if (PlainTextSource.this.current != null &&
            PlainTextSource.this.current.length() == 0) {
          PlainTextSource.this.current = PlainTextSource.this.readLine();
        }

        return new SentenceIterator() {
          private int idx = 0;

          public boolean hasNext() {
            return this.idx < sentences.size();
          }

          public Sentence<Token> next() {
            final int idx = this.idx++;
            return new Sentence(null) {
              private static final long serialVersionUID = 42L;
              public Iterator<Token> tokens() {
                return sentences.get(idx).iterator();
              }
            };
          }
        };
      }
    };
  }
}

