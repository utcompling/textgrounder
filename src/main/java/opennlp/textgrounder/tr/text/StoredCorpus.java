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

import opennlp.textgrounder.tr.topo.Location;
import opennlp.textgrounder.tr.util.CountingLexicon;
import opennlp.textgrounder.tr.util.SimpleCountingLexicon;
import opennlp.textgrounder.tr.util.Span;
import java.io.*;

public abstract class StoredCorpus extends Corpus<StoredToken> implements Serializable {
  public abstract int getDocumentCount();
  public abstract int getTokenTypeCount();
  public abstract int getTokenOrigTypeCount();
  public abstract int getToponymTypeCount();
  public abstract int getToponymOrigTypeCount();
  public abstract int getMaxToponymAmbiguity();
  public abstract double getAvgToponymAmbiguity();
  public abstract int getTokenCount();
  public abstract int getToponymTokenCount();
  public abstract void load();
}
