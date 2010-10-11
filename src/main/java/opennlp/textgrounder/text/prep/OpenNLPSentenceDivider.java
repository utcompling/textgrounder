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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;

import opennlp.textgrounder.util.Constants;

public class OpenNLPSentenceDivider implements SentenceDivider {
  private final SentenceDetector detector;

  public OpenNLPSentenceDivider() throws IOException, InvalidFormatException {
    this(new FileInputStream(Constants.getOpenNLPModelsDir() + File.separator + "en-sent.bin"));
  }

  public OpenNLPSentenceDivider(InputStream in) throws IOException, InvalidFormatException {
    this.detector = new SentenceDetectorME(new SentenceModel(in));
  }

  public List<String> divide(String text) {
    return Arrays.asList(this.detector.sentDetect(text));
  }
}

