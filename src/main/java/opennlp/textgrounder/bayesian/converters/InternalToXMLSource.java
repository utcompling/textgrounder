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
package opennlp.textgrounder.bayesian.converters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.DocumentSource;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.SimpleSentence;
import opennlp.textgrounder.text.Token;

/**
 * Modified by
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class InternalToXMLSource<T extends InternalToXMLConverterInterface> extends DocumentSource {

    private static final long serialVersionUID = 42L;

    protected final XMLStreamReader in;
    protected final XMLStreamWriter out;
    private final BufferedWriter writer;
    protected T converterInterfaceObject;

    public InternalToXMLSource(BufferedReader _reader, BufferedWriter _writer, T _converterInterfaceObject) throws XMLStreamException, IOException {
        converterInterfaceObject = _converterInterfaceObject;
        writer = _writer;

        XMLInputFactory ifactory = XMLInputFactory.newInstance();
        in = ifactory.createXMLStreamReader(_reader);

        XMLOutputFactory ofactory = XMLOutputFactory.newInstance();
        out = ofactory.createXMLStreamWriter(_writer);
        out.writeStartDocument("UTF-8", "1.0");

        while (in.hasNext() && in.next() != XMLStreamReader.START_ELEMENT) {
        }
        if (in.getLocalName().equals("corpus")) {
            writeElement();
            nextTag();
        }
    }

    public int nextTag() throws XMLStreamException, IOException {
        int nextel = in.nextTag();
        writeElement();
        return nextel;
    }

    @Override
    public void close() {
        try {
            in.close();

            out.writeEndElement();
            out.writeEndDocument();
            out.close();
        } catch (XMLStreamException e) {
            System.err.println("Error while closing XML file.");
        }
    }

    @Override
    public boolean hasNext() {
        return this.in.isStartElement() && this.in.getLocalName().equals("doc");
    }

    protected void writeElement() throws XMLStreamException, IOException {
        String name = in.getLocalName();
        if (in.isStartElement()) {
            out.writeStartElement(name);
            int c = in.getAttributeCount();
            for (int i = 0; i < c; ++i) {
                String attrname = in.getAttributeLocalName(i);
                String attrval = in.getAttributeValue(i);
                out.writeAttribute(attrname, attrval);
            }
        }
    }

    public void writeEndElement() throws XMLStreamException, IOException {
        out.writeEndElement();
        writer.newLine();
    }

    @Override
    public Document<Token> next() {
        assert this.in.isStartElement() && this.in.getLocalName().equals("doc");
        String id = InternalToXMLSource.this.in.getAttributeValue(null, "id");

        try {
            InternalToXMLSource.this.nextTag();
        } catch (XMLStreamException ex) {
            Logger.getLogger(InternalToXMLSource.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException e) {
        }

        return new Document(id) {

            private static final long serialVersionUID = 42L;

            @Override
            public Iterator<Sentence<Token>> iterator() {
                return new SentenceIterator() {

                    @Override
                    public boolean hasNext() {
                        if (InternalToXMLSource.this.in.isStartElement()
                              && InternalToXMLSource.this.in.getLocalName().equals("s")) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public Sentence<Token> next() {
                        String id = InternalToXMLSource.this.in.getAttributeValue(null, "id");

                        try {
                            while (InternalToXMLSource.this.nextTag() == XMLStreamReader.START_ELEMENT
                                  && (InternalToXMLSource.this.in.getLocalName().equals("w")
                                  || InternalToXMLSource.this.in.getLocalName().equals("toponym"))) {
                                String name = InternalToXMLSource.this.in.getLocalName();

                                if (name.equals("w")) {
                                    String tok = InternalToXMLSource.this.in.getAttributeValue(null, "tok");
                                    converterInterfaceObject.setCurrentWord(tok);
                                    converterInterfaceObject.setTokenAttribute(InternalToXMLSource.this.in, InternalToXMLSource.this.out);
                                } else {
                                    String toponym = InternalToXMLSource.this.in.getAttributeValue(null, "term");
                                    converterInterfaceObject.setCurrentWord(toponym);
                                    converterInterfaceObject.setToponymAttribute(InternalToXMLSource.this.in, InternalToXMLSource.this.out);

                                    if (InternalToXMLSource.this.nextTag() == XMLStreamReader.START_ELEMENT
                                          && InternalToXMLSource.this.in.getLocalName().equals("candidates")) {
                                        while (InternalToXMLSource.this.nextTag() == XMLStreamReader.START_ELEMENT
                                              && InternalToXMLSource.this.in.getLocalName().equals("cand")) {

                                            double lat = Double.parseDouble(InternalToXMLSource.this.in.getAttributeValue(null, "lat"));
                                            double lng = Double.parseDouble(InternalToXMLSource.this.in.getAttributeValue(null, "long"));
                                            converterInterfaceObject.confirmCoordinate(lat, lng, InternalToXMLSource.this.out);

                                            if (InternalToXMLSource.this.nextTag() == XMLStreamReader.START_ELEMENT
                                                  && InternalToXMLSource.this.in.getLocalName().equals("representatives")) {
                                                while (InternalToXMLSource.this.nextTag() == XMLStreamReader.START_ELEMENT
                                                      && InternalToXMLSource.this.in.getLocalName().equals("rep")) {
                                                    lat = Double.parseDouble(InternalToXMLSource.this.in.getAttributeValue(null, "lat"));
                                                    lng = Double.parseDouble(InternalToXMLSource.this.in.getAttributeValue(null, "long"));

                                                    /**
                                                     * add closing nextTag calls only to elements that can have sister nodes.
                                                     */
                                                    InternalToXMLSource.this.writeEndElement();
                                                    InternalToXMLSource.this.nextTag();
                                                    assert InternalToXMLSource.this.in.isEndElement()
                                                          && InternalToXMLSource.this.in.getLocalName().equals("rep");
                                                }
                                                assert InternalToXMLSource.this.in.isEndElement()
                                                      && InternalToXMLSource.this.in.getLocalName().equals("representatives");
                                                /**
                                                 * This weird kludge nextTag call is to accommodate the fact
                                                 * that the representatives element is optional and therefore
                                                 * must be walked out of if it does occur
                                                 */
                                                InternalToXMLSource.this.writeEndElement();
                                                InternalToXMLSource.this.nextTag();
                                            }
                                            InternalToXMLSource.this.writeEndElement();
                                            assert InternalToXMLSource.this.in.isEndElement()
                                                  && InternalToXMLSource.this.in.getLocalName().equals("cand");
                                        }
                                        InternalToXMLSource.this.writeEndElement();
                                        assert InternalToXMLSource.this.in.isEndElement()
                                              && InternalToXMLSource.this.in.getLocalName().equals("candidates");
                                    }
                                }
                                InternalToXMLSource.this.writeEndElement();
                                InternalToXMLSource.this.nextTag();
                                assert InternalToXMLSource.this.in.isEndElement()
                                      && (InternalToXMLSource.this.in.getLocalName().equals("w")
                                      || InternalToXMLSource.this.in.getLocalName().equals("toponym"));

                                converterInterfaceObject.incrementOffset();
                            }
                        } catch (XMLStreamException e) {
                            System.err.println("Error while reading XML file.");
                        } catch (IOException e) {
                        }
                        try {
                            InternalToXMLSource.this.writeEndElement();
                            InternalToXMLSource.this.nextTag();
                        } catch (XMLStreamException ex) {
                            Logger.getLogger(InternalToXMLSource.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (IOException e) {
                        }
                        return new SimpleSentence(id, null, null);
                    }
                };
            }
        };
    }
}
