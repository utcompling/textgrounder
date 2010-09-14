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
    private int id; // Identifier in a lexicon
    private boolean istop;
    private Location goldLocation; // Gold-standard location, if known 

    public Token(CorpusDocument doc, boolean istop) {
        super(doc, "SHOULD_NOT_BE_SEEN");
        this.istop = istop;
    }

    public Token(CorpusDocument doc, int id, boolean istop) {
        this(doc, istop);
        this.id = id;
    }

    public void setGoldLocation(Location goldLocation) {
      this.goldLocation = goldLocation;
    }

    public Location getGoldLocation() {
      return this.goldLocation;
    }

    protected void copyElementProperties(Element e) {
        istop = e.getName().equals("toponym");
        for (Attribute att : (List<Attribute>) e.getAttributes()) {
            String name = att.getName();
            String value = att.getValue();
            // System.out.println("name=" + name + ", value=" + value);
            if ((!istop && name.equals("tok")) || (istop && name.equals("term")))
                id = document.corpus.getLexicon().getOrAdd(value);
            else
                props.put(name, value);
        }
        assert (id != 0);
    }

    protected void writeElement(XMLStreamWriter w) throws XMLStreamException {
      w.writeStartElement(this.istop ? "toponym" : "w");
      for (String name : props.keySet()) {
        w.writeAttribute(name, props.get(name));
      }
      String word = this.document.corpus.getLexicon().atIndex(id);
      if (this.istop) {
        w.writeAttribute("term", word);
        w.writeStartElement("candidates");

        int[] locIds = document.corpus.getGazetteer().get(word).toArray();
        Arrays.sort(locIds);
        List<Location> locations = new ArrayList<Location>(locIds.length);
        for (int locId : locIds) {
          locations.add(document.corpus.getGazetteer().getLocation(locId));
        }

        for (Location location : locations) {
          w.writeStartElement("cand");
          w.writeAttribute("id", "c" + location.getId());

          Coordinate coord = location.getCoord();
          w.writeAttribute("lat", Double.toString(coord.latitude));
          w.writeAttribute("long", Double.toString(coord.longitude));
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
      assert this.size() == 0 : "There should be no children.";
    }
}

