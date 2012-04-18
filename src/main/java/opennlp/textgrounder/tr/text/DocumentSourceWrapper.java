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

import java.util.Iterator;

import opennlp.textgrounder.tr.text.Document;
import opennlp.textgrounder.tr.text.Sentence;
import opennlp.textgrounder.tr.text.Token;

/**
 * Wraps a document source in order to perform some operation on it.
 *
 * @author Travis Brown <travis.brown@mail.utexas.edu>
 * @version 0.1.0
 */
public abstract class DocumentSourceWrapper extends DocumentSource {
  private final DocumentSource source;

  public DocumentSourceWrapper(DocumentSource source) {
    this.source = source;
  }

  /**
   * Closes the underlying source.
   */
  public void close() {
    this.source.close();
  }

  /**
   * Indicates whether the underlying source has more documents.
   */
  public boolean hasNext() {
    return this.source.hasNext();
  }

  /**
   * Returns the underlying source (for use in subclasses).
   */
  protected DocumentSource getSource() {
    return this.source;
  }
}

