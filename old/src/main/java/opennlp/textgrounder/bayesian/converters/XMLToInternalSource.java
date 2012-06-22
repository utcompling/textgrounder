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

import java.io.Reader;

import java.util.Iterator;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.DocumentSource;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.SimpleSentence;
import opennlp.textgrounder.text.Token;

/**
 * Modified by
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class XMLToInternalSource<T extends XMLToInternalConverterInterface> extends DocumentSource {

    private final XMLStreamReader in;
    protected T converterInterfaceObject;

    public XMLToInternalSource(Reader _reader, T _converterInterfaceObject) throws XMLStreamException {
        converterInterfaceObject = _converterInterfaceObject;
        XMLInputFactory factory = XMLInputFactory.newInstance();
        this.in = factory.createXMLStreamReader(_reader);

        while (this.in.hasNext() && this.in.next() != XMLStreamReader.START_ELEMENT) {
        }
        if (this.in.getLocalName().equals("corpus")) {
            this.in.nextTag();
        }
    }

    public void nextTag() {
        try {
            this.in.nextTag();
        } catch (XMLStreamException e) {
            System.err.println("Error while advancing XML file.");
        }
    }

    @Override
    public void close() {
        try {
            this.in.close();
        } catch (XMLStreamException e) {
            System.err.println("Error while closing XML file.");
        }
    }

    @Override
    public boolean hasNext() {
        return this.in.isStartElement() && this.in.getLocalName().equals("doc");
    }

    @Override
    public Document<Token> next() {
        assert this.in.isStartElement() && this.in.getLocalName().equals("doc");
        String id = XMLToInternalSource.this.in.getAttributeValue(null, "id");
        XMLToInternalSource.this.nextTag();

        return new Document(id) {

            private static final long serialVersionUID = 42L;

            @Override
            public Iterator<Sentence<Token>> iterator() {
                return new SentenceIterator() {

                    @Override
                    public boolean hasNext() {
                        if (XMLToInternalSource.this.in.isStartElement()
                              && XMLToInternalSource.this.in.getLocalName().equals("s")) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public Sentence<Token> next() {
                        String id = XMLToInternalSource.this.in.getAttributeValue(null, "id");

                        try {
                            while (XMLToInternalSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                  && (XMLToInternalSource.this.in.getLocalName().equals("w")
                                  || XMLToInternalSource.this.in.getLocalName().equals("toponym"))) {
                                String name = XMLToInternalSource.this.in.getLocalName();

                                if (name.equals("w")) {
                                    converterInterfaceObject.addToken(XMLToInternalSource.this.in.getAttributeValue(null, "tok"));
                                } else {
                                    String toponym = XMLToInternalSource.this.in.getAttributeValue(null, "term");
                                    /**
                                     * We don't know whether a toponym has candidate
                                     * coordinates or not until the next elements in the stream
                                     * have been processed. But we do need to know
                                     * what the toponym is to map stuff to proper
                                     * structures if need. This is what the following
                                     * function call does.
                                     */
                                    converterInterfaceObject.setCurrentToponym(toponym);

                                    if (XMLToInternalSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                          && XMLToInternalSource.this.in.getLocalName().equals("candidates")) {
                                        while (XMLToInternalSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                              && XMLToInternalSource.this.in.getLocalName().equals("cand")) {

                                            double lat = Double.parseDouble(XMLToInternalSource.this.in.getAttributeValue(null, "lat"));
                                            double lng = Double.parseDouble(XMLToInternalSource.this.in.getAttributeValue(null, "long"));
                                            converterInterfaceObject.addCoordinate(lat, lng);

                                            if (XMLToInternalSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                                  && XMLToInternalSource.this.in.getLocalName().equals("representatives")) {
                                                while (XMLToInternalSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                                      && XMLToInternalSource.this.in.getLocalName().equals("rep")) {
                                                    lat = Double.parseDouble(XMLToInternalSource.this.in.getAttributeValue(null, "lat"));
                                                    lng = Double.parseDouble(XMLToInternalSource.this.in.getAttributeValue(null, "long"));
                                                    converterInterfaceObject.addRepresentative(lat, lng);

                                                    /**
                                                     * add closing nextTag calls only to elements that can have sister nodes.
                                                     */
                                                    XMLToInternalSource.this.nextTag();
                                                    assert XMLToInternalSource.this.in.isEndElement()
                                                          && XMLToInternalSource.this.in.getLocalName().equals("rep");
                                                }
                                                assert XMLToInternalSource.this.in.isEndElement()
                                                      && XMLToInternalSource.this.in.getLocalName().equals("representatives");
                                                /**
                                                 * This weird kludge nextTag call is to accommodate the fact
                                                 * that the representatives element is optional and therefore
                                                 * must be walked out of if it does occur
                                                 */
                                                XMLToInternalSource.this.nextTag();
                                            }
                                            assert XMLToInternalSource.this.in.isEndElement()
                                                  && XMLToInternalSource.this.in.getLocalName().equals("cand");
                                        }
                                        assert XMLToInternalSource.this.in.isEndElement()
                                              && XMLToInternalSource.this.in.getLocalName().equals("candidates");
                                    }
                                    converterInterfaceObject.addToponym(toponym);
                                }
                                XMLToInternalSource.this.nextTag();
                                assert XMLToInternalSource.this.in.isEndElement()
                                      && (XMLToInternalSource.this.in.getLocalName().equals("w")
                                      || XMLToInternalSource.this.in.getLocalName().equals("toponym"));
                            }
                        } catch (XMLStreamException e) {
                            System.err.println("Error while reading XML file.");
                        }

                        XMLToInternalSource.this.nextTag();
                        return new SimpleSentence(id, null, null);
                    }
                };
            }
        };
    }
}
