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

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

public abstract class DocumentSource implements Iterator<Document<Token>>, Iterable<Document<Token>>, Closeable {
  protected final Reader reader;

  public DocumentSource(Reader reader) throws IOException {
    this.reader = reader;
  }

  public Iterator<Document<Token>> iterator() {
    return this;
  }

  public void close() throws IOException {
    this.reader.close();
  }

  public void remove() {
    throw new UnsupportedOperationException("Cannot remove item from corpus source.");
  }
}

