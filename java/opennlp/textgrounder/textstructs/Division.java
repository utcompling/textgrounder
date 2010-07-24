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
package opennlp.textgrounder.textstructs;

/**
 * Class that stores data about a division of a document, greater than a single
 * token. This can include sentences, paragraphs, chapters, etc. The children of
 * a division are either other Divisions or Tokens.
 * 
 * @author benwing
 */
public class Division extends DocumentComponent {
    static private final long serialVersionUID = 1L;

    public Division(CorpusDocument doc, String type) {
        super(doc, type);
    }
}