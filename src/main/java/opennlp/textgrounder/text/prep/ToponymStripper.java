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
package opennlp.textgrounder.text.prep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.text.Corpus;
import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.DocumentSource;
import opennlp.textgrounder.text.DocumentSourceWrapper;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.SimpleSentence;
import opennlp.textgrounder.text.SimpleToponym;
import opennlp.textgrounder.text.Token;
import opennlp.textgrounder.text.Toponym;
import opennlp.textgrounder.topo.gaz.Gazetteer;
import opennlp.textgrounder.util.Span;

/**
 * Wraps a document source and removes any toponyms spans that it contains,
 * returning only the tokens.
 *
 * @author Travis Brown <travis.brown@mail.utexas.edu>
 * @version 0.1.0
 */
public class ToponymStripper extends DocumentSourceWrapper {
  public ToponymStripper(DocumentSource source) {
    super(source);
  }

  public Document<Token> next() {
    final Document<Token> document = this.getSource().next();
    final Iterator<Sentence<Token>> sentences = document.iterator();

    return new Document<Token>(document.getId()) {
      public Iterator<Sentence<Token>> iterator() {
        return new SentenceIterator() {
          public boolean hasNext() {
            return sentences.hasNext();
          }

          public Sentence<Token> next() {
            Sentence<Token> sentence = sentences.next();
            return new SimpleSentence(sentence.getId(), sentence.getTokens());
          }
        };
      }
    };
  }
}

