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
package opennlp.textgrounder.eval;

import java.util.Iterator;

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.Corpus;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.Token;

public class EDEvaluator extends Evaluator {
  public EDEvaluator(Corpus<Token> corpus) {
    super(corpus);
  }

  public Report evaluate() {
    return null;
  }

  public Report evaluate(Corpus<Token> pred, boolean useSelected) {
    Iterator<Document<Token>> goldDocs = this.corpus.iterator();
    Iterator<Document<Token>> predDocs = pred.iterator();

    while (goldDocs.hasNext() && predDocs.hasNext()) {
      Iterator<Sentence<Token>> goldSents = goldDocs.next().iterator();
      Iterator<Sentence<Token>> predSents = predDocs.next().iterator();

      while (goldSents.hasNext() && predSents.hasNext()) {
      }

      assert !goldSents.hasNext() && !predSents.hasNext() : "Documents have different numbers of sentences.";
    }

    assert !goldDocs.hasNext() && !predDocs.hasNext() : "Corpora have different numbers of documents.";
    return null;
  }
}

