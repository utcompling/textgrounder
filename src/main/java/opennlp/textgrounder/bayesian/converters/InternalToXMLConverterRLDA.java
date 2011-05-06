///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Taesun Moon <tsunmoon@gmail.com>.
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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.topostructs.*;
import opennlp.textgrounder.bayesian.wrapper.io.*;
import org.jdom.Element;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class InternalToXMLConverterRLDA extends InternalToXMLConverter {

    /**
     *
     */
    protected int degreesPerRegion;
    /**
     *
     */
    protected Region[][] regionMatrix;
    /**
     * 
     */
    protected HashMap<Integer, Region> regionIdToRegionMap;
    /**
     * 
     */
    protected ToponymToRegionIDsMap toponymToRegionIDsMap;

    /**
     *
     * @param _converterExperimentParameters
     */
    public InternalToXMLConverterRLDA(
          ConverterExperimentParameters _converterExperimentParameters) {
        super(_converterExperimentParameters);
    }

    @Override
    public void initialize() {
        /**
         * initialize various fields
         */
        regionIdToRegionMap = new HashMap<Integer, Region>();

        /**
         * Read in binary data
         */
        InputReader inputReader = new BinaryInputReader(converterExperimentParameters);

        lexicon = inputReader.readLexicon();
        regionMatrix = inputReader.readRegions();

        for (Region[] regions : regionMatrix) {
            for (Region region : regions) {
                if (region != null) {
                    regionIdToRegionMap.put(region.id, region);
                }
            }
        }

        /**
         * Read in processed tokens
         */
        wordArray = new ArrayList<Integer>();
        docArray = new ArrayList<Integer>();
        toponymArray = new ArrayList<Integer>();
        stopwordArray = new ArrayList<Integer>();
        regionArray = new ArrayList<Integer>();

        try {
            while (true) {
                int[] record = inputReader.nextTokenArrayRecord();
                if (record != null) {
                    int wordid = record[0];
                    wordArray.add(wordid);
                    int docid = record[1];
                    docArray.add(docid);
                    int topstatus = record[2];
                    toponymArray.add(topstatus);
                    int stopstatus = record[3];
                    stopwordArray.add(stopstatus);
                    int regid = record[4];
                    regionArray.add(regid);
                }
            }
        } catch (EOFException ex) {
        } catch (IOException ex) {
            Logger.getLogger(InternalToXMLConverterRLDA.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    protected Coordinate matchCandidate(ArrayList<Element> _candidates,
          int _regionid) {
        Region candregion = regionIdToRegionMap.get(_regionid);
        Coordinate candcoord = new Coordinate(candregion.centLon, candregion.centLat);

        for (Element candidate : _candidates) {
            double lon = Double.parseDouble(candidate.getAttributeValue("long"));
            double lat = Double.parseDouble(candidate.getAttributeValue("lat"));

            if (lon <= candregion.maxLon && lon >= candregion.minLon) {
                if (lat <= candregion.maxLat && lat >= candregion.minLat) {
                    candcoord = new Coordinate(lon, lat);
                    break;
                }
            }
        }

        return candcoord;
    }

    @Override
    public void confirmCoordinate(double _long, double _lat, XMLStreamWriter _out) throws XMLStreamException {
//        if (!_candidates.isEmpty()) {
//            Coordinate coord = matchCandidate(_candidates, _regid);
//            _token.setAttribute("long", String.format("%.2f", coord.longitude));
//            _token.setAttribute("lat", String.format("%.2f", coord.latitude));
//        }
    }

    @Override
    public void setTokenAttribute(XMLStreamReader in, XMLStreamWriter out) throws XMLStreamException {
        setToponymAttribute(in, out, "tok");
    }

    @Override
    public void setToponymAttribute(XMLStreamReader in, XMLStreamWriter out) throws XMLStreamException {
        setToponymAttribute(in, out, "term");
    }

    protected void setToponymAttribute(XMLStreamReader in, XMLStreamWriter out, String _attr) throws XMLStreamException {
        int isstopword = stopwordArray.get(offset);
        int wordid = wordArray.get(offset);

        if (isstopword == 0) {
            String word = in.getAttributeValue(null, _attr);
            String outword = lexicon.getWordForInt(wordid);
            if (word.toLowerCase().equals(outword)) {
                int regid = regionArray.get(offset);

                Region reg = regionIdToRegionMap.get(regid);
                out.writeAttribute("long", String.format("%.2f", reg.centLon));
                out.writeAttribute("lat", String.format("%.2f", reg.centLat));
            } else {
                int outdocid = docArray.get(offset);
                System.err.println(String.format("Mismatch between "
                      + "tokens. Occurred at source document %s, "
                      + "sentence %s, token %s and target document %d, "
                      + "offset %d, token %s, token id %d",
                      currentDocumentID, currentSentenceID, word, outdocid, offset, outword, wordid));
                System.exit(1);
            }
        }
    }

    @Override
    public void setCurrentDocumentID(String _string) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setCurrentSentenceID(String _string) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
