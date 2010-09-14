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

import gnu.trove.TIntIterator;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

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
    public Location goldLocation; // Gold-standard location, if known 
    
    public Token(CorpusDocument doc, boolean istop) {
        super(doc, "SHOULD_NOT_BE_SEEN");
        this.istop = istop;
    }
    
    public Token(CorpusDocument doc, int id, boolean istop) {
        this(doc, istop);
        this.id = id;
    }
    
    protected void copyElementProperties(Element e) {
        istop = e.getName().equals("toponym");
        for (Attribute att : (List<Attribute>) e.getAttributes()) {
            String name = att.getName();
            String value = att.getValue();
            // System.out.println("name=" + name + ", value=" + value);
            if ((!istop && name.equals("tok")) || (istop && name.equals("term")))
                id = document.corpus.lexicon.addWord(value);
            else
                props.put(name, value);
        }
        assert (id != 0);
    }

    protected void writeElement(XMLStreamWriter w) throws XMLStreamException {
      w.writeStartElement(this.istop ? "toponym" : "w");
      // copy properties
      for (String name : props.keySet()) {
        w.writeAttribute(name, props.get(name));
      }
      String word = this.document.corpus.lexicon.getWordForInt(id);
      if (this.istop) {
        w.writeAttribute("term", word);
        w.writeStartElement("candidates");

        int[] locIds = document.corpus.gazetteer.get(word).toArray();
        Arrays.sort(locIds);
        List<Location> locations = new ArrayList<Location>(locIds.length);
        for (int locId : locIds) {
          locations.add(document.corpus.gazetteer.getLocation(locId));
        }

        for (Location location : locations) {
          w.writeStartElement("cand");
          w.writeAttribute("id", "c" + location.getId());

          Coordinate coord = location.getCoord();
          /* Java sucks.  Why can't I just call toString() on a double? */
          w.writeAttribute("lat", "" + coord.latitude);
          w.writeAttribute("long", "" + coord.longitude);
          if (location.getType() != null) {
            w.writeAttribute("type", location.getType());
          }
          if (location.getContainer() != null) {
            w.writeAttribute("container", location.getContainer());
          }
          if (location.getPop() > 0) {
            w.writeAttribute("population", "" + location.getPop());
          }
          w.writeEndElement();
        }
        w.writeEndElement();
      } else {
        assert (word != null);
        w.writeAttribute("tok", word);
      }
      w.writeEndElement();
      // there should be no children
      for (DocumentComponent child : this) {
        assert (false);
      }
    }

    /**
     * Create a new XML Element corresponding to the current component
     * (including its children).
     */
    protected Element outputElement() {
        Element e = new Element(istop ? "toponym" : "w");
        String word = document.corpus.lexicon.getWordForInt(id);
        if (istop) {
            e.setAttribute("term", word);
            Element cands = new Element("candidates");
            e.addContent(cands);

            int[] locIds = document.corpus.gazetteer.get(word).toArray();
            Arrays.sort(locIds);
            List<Location> locations = new ArrayList<Location>(locIds.length);
            for (int locId : locIds) {
              locations.add(document.corpus.gazetteer.getLocation(locId));
            }

            for (Location location : locations) {
                Element cand = new Element("cand");
                cands.addContent(cand);
                cand.setAttribute("id", "c" + location.getId());
                Coordinate coord = location.getCoord();
                /* Java sucks.  Why can't I just call toString() on a double? */
                cand.setAttribute("lat", "" + coord.latitude);
                cand.setAttribute("long", "" + coord.longitude);
                if (location.getType() != null)
                    cand.setAttribute("type", location.getType());
                if (location.getContainer() != null)
                    cand.setAttribute("container", location.getContainer());
                if (location.getPop() > 0)
                    cand.setAttribute("population", "" + location.getPop());
            }
        } else {
            assert (word != null);
            e.setAttribute("tok", word);
        }
        // copy properties
        for (String name : props.keySet())
            e.setAttribute(name, props.get(name));
        // there should be no children
        for (DocumentComponent child : this)
            assert (false);
        return e;
    }
}

