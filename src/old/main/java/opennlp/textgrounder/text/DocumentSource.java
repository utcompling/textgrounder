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

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.Token;

public abstract class DocumentSource implements Iterator<Document<Token>> {
  public void close() {
  }

  public void remove() {
    throw new UnsupportedOperationException("Cannot remove a document from a source.");
  }

  protected abstract class SentenceIterator implements Iterator<Sentence<Token>> {
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove a sentence from a source.");
    }    
  }
}

