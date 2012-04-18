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
package opennlp.textgrounder.tr.topo.gaz;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public abstract class GazetteerFileReader extends GazetteerReader {
  private final BufferedReader reader;

  protected GazetteerFileReader(BufferedReader reader)
    throws FileNotFoundException, IOException {
    this.reader = reader;
  }

  protected String readLine() {
    String line = null;
    try {
      line = this.reader.readLine();
    } catch (IOException e) {   
      System.err.format("Error while reading gazetteer file: %s\n", e);
      e.printStackTrace();
    }
    return line;
  }

  public void close() {
    try {
      this.reader.close();
    } catch (IOException e) {      
      System.err.format("Error closing gazetteer file: %s\n", e);
      e.printStackTrace();
      System.exit(1); 
    }
  }
}

