///////////////////////////////////////////////////////////////////////////////
//  Copyright 2011 Taesun Moon <tsunmoon@gmail.com>.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.bayesian.converters;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public interface InternalToXMLConverterInterface {

    public void confirmCoordinate(double _lat, double _long, XMLStreamWriter out) throws XMLStreamException;

    public void setCurrentWord(String _string);

    public void setCurrentDocumentID(String _string);

    public void setCurrentSentenceID(String _string);

    public void incrementOffset();

    public void setTokenAttribute(XMLStreamReader in, XMLStreamWriter out) throws XMLStreamException;

    public void setToponymAttribute(XMLStreamReader in, XMLStreamWriter out) throws XMLStreamException;
}
