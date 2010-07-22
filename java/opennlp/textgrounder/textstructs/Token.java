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

import java.util.List;

import org.jdom.Attribute;
import org.jdom.Element;

import opennlp.textgrounder.topostructs.*;

/**
 * Class that stores data about a single token in a sequence of tokens. Tokens
 * are generally children of Divisions.
 * 
 * NOTE: A single token may correspond to multiple words, particularly in the
 * case of multi-word place names.
 * 
 * @author benwing
 */
public class Token extends DocumentComponent {
    static private final long serialVersionUID = 1L;

    public int id; // Identifier in a lexicon
    public boolean istop;
    public String netype; // NER type, if known
    public Location goldLocation; // Gold-standard location, if known 
    
    public Token(CorpusDocument doc) {
        super(doc, "w");
    }
    
    public Token(CorpusDocument doc, int id, boolean istop) {
        this(doc);
        this.id = id;
        this.istop = istop;
        props.put("tok", doc.corpus.lexicon.getWordForInt(id));
    }
    
    protected void copyElementProperties(Element e) {
        for (Attribute att : (List<Attribute>) e.getAttributes()) {
            String name = att.getName();
            String value = att.getValue();
            if (name.equals("tok"))
                id = document.corpus.lexicon.addWord(value);
            else if (name.equals("ne"))
                netype = value;
            props.put(name, value);
        }
    }

}
