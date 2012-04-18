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
package opennlp.textgrounder.tr.text.prep;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InvalidFormatException;

import opennlp.textgrounder.tr.util.Constants;

public class OpenNLPTokenizer implements Tokenizer {
  private final opennlp.tools.tokenize.Tokenizer tokenizer;

  public OpenNLPTokenizer() throws IOException, InvalidFormatException {
    this(new FileInputStream(Constants.getOpenNLPModelsDir() + File.separator + "en-token.bin"));
  }

  public OpenNLPTokenizer(InputStream in) throws IOException, InvalidFormatException {
    this.tokenizer = new TokenizerME(new TokenizerModel(in));
  }

  public List<String> tokenize(String text) {
    return Arrays.asList(this.tokenizer.tokenize(text));
  }
}

