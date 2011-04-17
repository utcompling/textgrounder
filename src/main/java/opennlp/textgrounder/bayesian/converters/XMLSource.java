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

import opennlp.textgrounder.text.io.*;
import java.io.Reader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import opennlp.textgrounder.text.Corpus;
import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.DocumentSource;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.SimpleSentence;
import opennlp.textgrounder.text.SimpleToken;
import opennlp.textgrounder.text.SimpleToponym;
import opennlp.textgrounder.text.Token;
import opennlp.textgrounder.text.Toponym;
import opennlp.textgrounder.text.prep.Tokenizer;
import opennlp.textgrounder.topo.Coordinate;
import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.topo.PointRegion;
import opennlp.textgrounder.topo.Region;
import opennlp.textgrounder.util.Span;

/**
 * Modified by
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class XMLSource <T extends ConverterInterface> extends DocumentSource {

    private final XMLStreamReader in;
    protected XMLToInternalConverter xmlToInternalConverter;
    protected T ci;

    public XMLSource (Reader _reader, T _ci) throws XMLStreamException {
        ci = _ci;
        XMLInputFactory factory = XMLInputFactory.newInstance();
        this.in = factory.createXMLStreamReader(_reader);

        while (this.in.hasNext() && this.in.next() != XMLStreamReader.START_ELEMENT) {
        }
        if (this.in.getLocalName().equals("corpus")) {
            this.in.nextTag();
        }
    }

//    public XMLSource(Reader _reader, XMLToInternalConverter _xmlToInternalConverter) throws XMLStreamException {
//        xmlToInternalConverter = _xmlToInternalConverter;
//
//        XMLInputFactory factory = XMLInputFactory.newInstance();
//        this.in = factory.createXMLStreamReader(_reader);
//
//        while (this.in.hasNext() && this.in.next() != XMLStreamReader.START_ELEMENT) {
//        }
//        if (this.in.getLocalName().equals("corpus")) {
//            this.in.nextTag();
//        }
//    }

    private void nextTag() {
        try {
            this.in.nextTag();
        } catch (XMLStreamException e) {
            System.err.println("Error while advancing XML file.");
        }
    }

    public void close() {
        try {
            this.in.close();
        } catch (XMLStreamException e) {
            System.err.println("Error while closing XML file.");
        }
    }

    public boolean hasNext() {
        return this.in.isStartElement() && this.in.getLocalName().equals("doc");
    }

    public Document<Token> next() {
        assert this.in.isStartElement() && this.in.getLocalName().equals("doc");
        String id = XMLSource.this.in.getAttributeValue(null, "id");
        XMLSource.this.nextTag();

        return new Document(id) {

            public Iterator<Sentence<Token>> iterator() {
                return new SentenceIterator() {

                    public boolean hasNext() {
                        if (XMLSource.this.in.isStartElement()
                              && XMLSource.this.in.getLocalName().equals("s")) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                    public Sentence<Token> next() {
                        String id = XMLSource.this.in.getAttributeValue(null, "id");

                        try {
                            while (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                  && (XMLSource.this.in.getLocalName().equals("w")
                                  || XMLSource.this.in.getLocalName().equals("toponym"))) {
                                String name = XMLSource.this.in.getLocalName();

                                if (name.equals("w")) {
                                    ci.addToken(XMLSource.this.in.getAttributeValue(null, "tok"));
                                } else {
                                    String toponym = XMLSource.this.in.getAttributeValue(null, "term");
                                    ci.addToponym(toponym);

                                    if (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                          && XMLSource.this.in.getLocalName().equals("candidates")) {
                                        while (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                              && XMLSource.this.in.getLocalName().equals("cand")) {

                                            double lat = Double.parseDouble(XMLSource.this.in.getAttributeValue(null, "lat"));
                                            double lng = Double.parseDouble(XMLSource.this.in.getAttributeValue(null, "long"));
                                            ci.addCoordinate(lng, lat);

                                            while (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                                  && XMLSource.this.in.getLocalName().equals("representatives")) {
                                                while (XMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT
                                                      && XMLSource.this.in.getLocalName().equals("rep")) {
                                                    XMLSource.this.nextTag();
                                                    assert XMLSource.this.in.isEndElement()
                                                          && XMLSource.this.in.getLocalName().equals("rep");
                                                }
                                                XMLSource.this.nextTag();
                                                assert XMLSource.this.in.isEndElement()
                                                      && XMLSource.this.in.getLocalName().equals("representatives");
                                            }
                                            XMLSource.this.nextTag();
                                            assert XMLSource.this.in.isEndElement()
                                                  && XMLSource.this.in.getLocalName().equals("cand");
                                        }
                                    }
                                }
                                XMLSource.this.nextTag();
                            }
                        } catch (XMLStreamException e) {
                            System.err.println("Error while reading TR-XML file.");
                        }

                        XMLSource.this.nextTag();
                        return new SimpleSentence(id, null, null);
                    }
                };
            }
        };
    }
}
