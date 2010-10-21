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
import java.io.File;
import java.io.FilenameFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.google.common.collect.Iterators;

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.DocumentSource;
import opennlp.textgrounder.text.Token;
import opennlp.textgrounder.text.prep.Tokenizer;

public class TrXMLDirSource extends DocumentSource {
  private final Tokenizer tokenizer;
  private final File[] files;
  private int currentIdx;
  private TrXMLSource current;

  public TrXMLDirSource(File directory, Tokenizer tokenizer) {
    this.tokenizer = tokenizer;
    File[] files = directory.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".xml");
      }
    });

    this.files = files == null ? new File[0] : files;
    Arrays.sort(this.files);

    this.currentIdx = 0;
    this.nextFile();
  }

  private void nextFile() {
    try {
      if (this.current != null) {
        this.current.close();
      }
      if (this.currentIdx < this.files.length) {
        this.current = new TrXMLSource(new BufferedReader(new FileReader(this.files[this.currentIdx])), this.tokenizer);
      }
    } catch (XMLStreamException e) {
      System.err.println("Error while reading TR-XML directory file.");
    } catch (FileNotFoundException e) {
      System.err.println("Error while reading TR-XML directory file.");
    }
  }

  public void close() {
    if (this.current != null) {
      this.current.close();
    }
  }

  public boolean hasNext() {
    if (this.currentIdx < this.files.length) {
      if (this.current.hasNext()) {
        return true; 
      } else {
        this.currentIdx++;
        this.nextFile();
        return this.hasNext();
      }
    } else {
      return false;
    }
  }

  public Document<Token> next() {
    return this.current.next();
  }
}

