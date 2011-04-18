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
public class XMLSource<T extends ConverterInterface> extends DocumentSource {

    private final XMLStreamReader in;
    protected T converterInterfaceObject;

    public XMLSource(Reader _reader, T _ci) throws XMLStreamException {
        converterInterfaceObject = _ci;
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
        String id = XMLSource.this.in.getAttributeValue(null, "id");
        XMLSource.this.nextTag();

        return new Document(id) {

            @Override
            public Iterator<Sentence<Token>> iterator() {
                return new SentenceIterator() {

                    @Override
                    public boolean hasNext() {
                        if (XMLSource.this.in.isStartElement()
                              && XMLSource.this.in.getLocalName().equals("s")) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public Sentence<Token> next() {
                        String id = XMLSource.this.in.getAttributeValue(null, "id");

                        try {
                            while (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                  && (XMLSource.this.in.getLocalName().equals("w")
                                  || XMLSource.this.in.getLocalName().equals("toponym"))) {
                                String name = XMLSource.this.in.getLocalName();

                                if (name.equals("w")) {
                                    converterInterfaceObject.addToken(XMLSource.this.in.getAttributeValue(null, "tok"));
                                } else {
                                    String toponym = XMLSource.this.in.getAttributeValue(null, "term");

                                    if (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                          && XMLSource.this.in.getLocalName().equals("candidates")) {
                                        while (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                              && XMLSource.this.in.getLocalName().equals("cand")) {

                                            double lng = Double.parseDouble(XMLSource.this.in.getAttributeValue(null, "long"));
                                            double lat = Double.parseDouble(XMLSource.this.in.getAttributeValue(null, "lat"));
                                            converterInterfaceObject.addCoordinate(lng, lat);

                                            if (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                                  && XMLSource.this.in.getLocalName().equals("representatives")) {
                                                while (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                                      && XMLSource.this.in.getLocalName().equals("rep")) {
                                                    lng = Double.parseDouble(XMLSource.this.in.getAttributeValue(null, "long"));
                                                    lat = Double.parseDouble(XMLSource.this.in.getAttributeValue(null, "lat"));
                                                    converterInterfaceObject.addRepresentative(lng, lat);

                                                    /**
                                                     * add closing nextTag calls only to elements that can have sister nodes.
                                                     */
                                                    XMLSource.this.nextTag();
                                                    assert XMLSource.this.in.isEndElement()
                                                          && XMLSource.this.in.getLocalName().equals("rep");
                                                }
                                                assert XMLSource.this.in.isEndElement()
                                                      && XMLSource.this.in.getLocalName().equals("representatives");
                                                /**
                                                 * This weird kludge nextTag call is to accommodate the fact
                                                 * that the representatives element is optional and therefore
                                                 * must be walked out of if it does occur
                                                 */
                                                XMLSource.this.nextTag();
                                            }
                                            assert XMLSource.this.in.isEndElement()
                                                  && XMLSource.this.in.getLocalName().equals("cand");
                                        }
                                        assert XMLSource.this.in.isEndElement()
                                              && XMLSource.this.in.getLocalName().equals("candidates");
                                    }
                                    converterInterfaceObject.addToponym(toponym);
                                }
                                XMLSource.this.nextTag();
                                assert XMLSource.this.in.isEndElement()
                                      && (XMLSource.this.in.getLocalName().equals("w")
                                      || XMLSource.this.in.getLocalName().equals("toponym"));
                            }
                        } catch (XMLStreamException e) {
                            System.err.println("Error while reading XML file.");
                        }

                        XMLSource.this.nextTag();
                        return new SimpleSentence(id, null, null);
                    }
                };
            }
        };
    }
}
