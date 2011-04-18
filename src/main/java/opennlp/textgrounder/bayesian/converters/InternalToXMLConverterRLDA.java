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
    protected void setTokenAttribute(Element _token, int _wordid, int _regid, int _coordid) {
        Region reg = regionIdToRegionMap.get(_regid);
        _token.setAttribute("long", String.format("%.2f", reg.centLon));
        _token.setAttribute("lat", String.format("%.2f", reg.centLat));
    }

    @Override
    protected void setToponymAttribute(ArrayList<Element> _candidates, Element _token, int _wordid, int _regid, int _coordid, int _offset) {
        if (!_candidates.isEmpty()) {
            Coordinate coord = matchCandidate(_candidates, _regid);
            _token.setAttribute("long", String.format("%.2f", coord.longitude));
            _token.setAttribute("lat", String.format("%.2f", coord.latitude));
        }
    }

    @Override
    public void addToken(String _string) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addToponym(String _string) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addCoordinate(double _long, double _lat) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addCandidate(double _long, double _lat) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addRepresentative(double _long, double _lat) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setCurrentToponym(String _string) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
