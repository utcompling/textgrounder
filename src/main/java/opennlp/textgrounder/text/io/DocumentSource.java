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
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.Token;

public abstract class DocumentSource implements Iterator<Document<Token>>, Iterable<Document<Token>>, Closeable {
  protected final BufferedReader reader;

  public DocumentSource(BufferedReader reader) throws IOException {
    this.reader = reader;
  }

  public Iterator<Document<Token>> iterator() {
    return this;
  }

  protected String readLine() {
    String line = null;
    try {
      line = this.reader.readLine();
    } catch (IOException e) {
      System.err.println("Error reading document source.");
    }
    return line;
  }

  public void close() throws IOException {
    this.reader.close();
  }

  public void remove() {
    throw new UnsupportedOperationException("Cannot remove item from corpus source.");
  }
}

