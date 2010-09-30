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
package opennlp.textgrounder.text.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.SimpleToken;
import opennlp.textgrounder.text.Token;
//import opennlp.textgrounder.text.ner.NamedEntityRecognizer;
import opennlp.textgrounder.text.prep.Tokenizer;
import opennlp.textgrounder.text.prep.SentenceDivider;
//import opennlp.textgrounder.topo.gaz.Gazetteer;

public abstract class PlainTextDocumentSource extends DocumentSource {
  private final SentenceDivider divider;
  private final Tokenizer tokenizer;
  //private final NamedEntityRecognizer recognizer;
  //private final Gazetteer gazetteer;

  private int number;
  private String current;
  private int currentIdx;
  private int parasPerDocument;

  public PlainTextDocumentSource(BufferedReader reader, SentenceDivider divider, Tokenizer tokenizer)
    throws IOException {
    super(reader);
    this.divider = divider;
    this.tokenizer = tokenizer;
    //this.recognizer = recognizer;
    //this.gazetteer = gazetteer;
    this.current = this.readLine();
    this.number = 0;
    this.currentIdx = 0;
    this.parasPerDocument = 5;
  }

  public boolean hasNext() {
    return this.current != null;
  }

  public Document<Token> next() {
    return new Document<Token>(null) {
      public Iterator<Sentence<Token>> iterator() {
        final List<List<Token>> sentences = new ArrayList<List<Token>>();
        while (PlainTextDocumentSource.this.current != null &&
               PlainTextDocumentSource.this.current.length() > 0 &&
               PlainTextDocumentSource.this.number < PlainTextDocumentSource.this.parasPerDocument) {
          for (String sentence : PlainTextDocumentSource.this.divider.divide(PlainTextDocumentSource.this.current)) {
            List<Token> tokens = new ArrayList<Token>();
            for (String token : PlainTextDocumentSource.this.tokenizer.tokenize(sentence)) {
              tokens.add(new SimpleToken(token));
            }
            sentences.add(tokens);
          }
          PlainTextDocumentSource.this.number++;
          PlainTextDocumentSource.this.current = PlainTextDocumentSource.this.readLine();
        }
        PlainTextDocumentSource.this.number = 0;
        if (PlainTextDocumentSource.this.current != null &&
            PlainTextDocumentSource.this.current.length() == 0) {
          PlainTextDocumentSource.this.current = PlainTextDocumentSource.this.readLine();
        }

        return new Iterator<Sentence<Token>>() {
          private int idx = 0;

          public boolean hasNext() {
            return this.idx < sentences.size();
          }

          public Sentence<Token> next() {
            final int idx = this.idx++;
            return new Sentence(null) {
              public Iterator<Token> tokens() {
                return sentences.get(idx).iterator();
              }
            };
          }
          
          public void remove() {
            throw new UnsupportedOperationException("Cannot remove item from corpus source.");
          }
        };
      }
    };
  }
}

