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

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.Token;
import opennlp.textgrounder.text.ner.NamedEntityRecognizer;
import opennlp.textgrounder.text.prep.Tokenizer;
import opennlp.textgrounder.text.prep.SentenceDivider;
import opennlp.textgrounder.topo.gaz.Gazetteer;

public abstract class PlainTextDocumentSource extends DocumentSource {
  private final Tokenizer tokenizer;
  private final SentenceDivider divider;
  private final NamedEntityRecognizer recognizer;
  private final Gazetteer gazetteer;

  public PlainTextDocumentSource(Reader reader, Tokenizer tokenizer,
                                 SentenceDivider divider, NamedEntityRecognizer recognizer, Gazetteer gazetteer)
    throws IOException {
    super(reader);
    this.tokenizer = tokenizer;
    this.divider = divider;
    this.recognizer = recognizer;
    this.gazetteer = gazetteer;
  }

  public boolean hasNext() {
    return false;
  }

  public Document<Token> next() {
    return null;
  }
}

