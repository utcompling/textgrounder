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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdom.Attribute;
import org.jdom.Element;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Class that stores data about some component of a document, including possibly
 * the document itself. Takes an XML-like view of a document as a nested series
 * of components, each with a type, a set of property/value pairs, and a list of
 * children.  Generally, a document component maps directly to an XML element.
 * 
 * @author benwing
 */
public abstract class DocumentComponent extends ArrayList<DocumentComponent> {
    /**
     * Type of component. Corresponds to name of corresponding XML element.
     */
    protected final String type; 
    /**
     * Properties of component. Corresponds to attributes and corresponding
     * values of the corresponding XML element.
     */
    protected final Map<String, String> props = new HashMap<String, String>();
    /**
     * Back pointer to document this component is part of.
     */
    protected CorpusDocument document;
    
    /**
     * Create a component of the given type.
     * 
     * @param type
     */
    public DocumentComponent(CorpusDocument document, String type) {
        super();
        this.document = document;
        this.type = type;
    }

    public boolean add(DocumentComponent o) {
        if (o instanceof Token) {
            document.tokens.add((Token) o); 
        }
        return super.add(o);
    }

    public void addProperty(String name, String value) {
      this.props.put(name, value);
    }

    protected void writeElement(XMLStreamWriter w) throws XMLStreamException {
      w.writeStartElement(this.type);
      for (String name : this.props.keySet()) {
        w.writeAttribute(name, props.get(name));
      }
      for (DocumentComponent child : this) {
        child.writeElement(w);
      }
      w.writeEndElement();
    }

    /**
     * Process an XML Element corresponding to the current component and
     * populate properties and children.
     * 
     * @param e The element being processed.
     */
    protected void processElement(Element e) {
        DocumentComponent comp;

        // copy properties
        copyElementProperties(e);
        // process children
        for (Element child : (List<Element>) e.getChildren()) {
            String elname = child.getName();
            if (elname.equals("w"))
                comp = new Token(document, false);
            else if (elname.equals("toponym"))
                comp = new Token(document, true);
            else
                comp = new Division(document, elname);
            comp.processElement(child);
            add(comp);
        }
    }
    
    /**
     * Copy the properties of the given element into the component.
     * 
     * @param e The element being processed.
     */
    protected void copyElementProperties(Element e) {
        // copy properties
        for (Attribute att : (List<Attribute>) e.getAttributes())
            props.put(att.getName(), att.getValue());
    }
    
    public String toString() {
        String retval = "<" + type;
        // copy properties
        for (String name : props.keySet())
            retval += " " + name + "=" + props.get(name);
        return retval + "/>";
    }
}
