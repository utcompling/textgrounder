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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.util.Span;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.InvalidFormatException;

public class OpenNLPRecognizer implements NamedEntityRecognizer {
  private final TokenNameFinder finder;
  private final NamedEntityType type;

  public OpenNLPRecognizer(InputStream in, NamedEntityType type) throws IOException, InvalidFormatException {
    this.finder = new NameFinderME(new TokenNameFinderModel(in));
    this.type = type;
  }

  public List<Span<NamedEntityType>> recognize(List<String> tokens) {
    List<Span<NamedEntityType>> spans = new ArrayList<Span<NamedEntityType>>();
    for (opennlp.tools.util.Span span : this.finder.find(tokens.toArray(new String[0]))) {
      spans.add(new Span<NamedEntityType>(span.getStart(), span.getEnd(), this.type));
    }
    return spans;
  }
}

