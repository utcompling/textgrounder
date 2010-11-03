///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Ben Wing, The University of Texas at Austin
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
package opennlp.textgrounder.old.ners;

import opennlp.textgrounder.old.textstructs.*;
/**
 * Class that processes raw text using a named entity recognizer (NER).  The
 * NER identifies the words in the raw text and tags each one with a tag
 * indicating whether or not it is a location.  The class is given raw text,
 * runs it through the NER, identifies the tokens (generally words, but may
 * be multi-word sequences in the case of locations such as "New York"), and
 * add the tokens to TokenArrayBuffer.
 */
public abstract class NamedEntityRecognizer {
    /**
     * Default constructor.
     * 
     * @param tokenArrayBuffer Buffer that holds the array of token indexes,
     * document indexes, and the toponym indexes.
     */
    public NamedEntityRecognizer() {
    }

    /**
     *
     */
    public abstract void processText(CorpusDocument doc, String text);
}
