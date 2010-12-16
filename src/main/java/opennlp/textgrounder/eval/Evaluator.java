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

import opennlp.textgrounder.text.Corpus;
import opennlp.textgrounder.text.Token;

public abstract class Evaluator {
  protected final Corpus<Token> corpus;

  /* The given corpus should include either gold or selected candidates or
   * both. */
  public Evaluator(Corpus<? extends Token> corpus) {
    this.corpus = (Corpus<Token>) corpus;
  }

  /* Evaluate the "selected" candidates in the corpus using its "gold"
   * candidates. */
  public abstract Report evaluate();

  /* Evaluate the given corpus using either the gold or selected candidates in
   * the current corpus. */
  public abstract Report evaluate(Corpus<Token> pred, boolean useSelected);

  /* A convenience method providing a default for evaluate. */
  public Report evaluate(Corpus<Token> pred) {
    return this.evaluate(pred, false);
  }
}

