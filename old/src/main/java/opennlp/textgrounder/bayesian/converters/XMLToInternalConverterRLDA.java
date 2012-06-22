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

import java.util.HashSet;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.wrapper.io.*;
import opennlp.textgrounder.bayesian.topostructs.*;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class XMLToInternalConverterRLDA extends XMLToInternalConverter {

    /**
     *
     */
    protected int activeRegions;
    /**
     *
     */
    protected int degreesPerRegion;
    /**
     *
     */
    protected Region[][] regionArray;
    /**
     * 
     */
    protected ToponymToRegionIDsMap toponymToRegionIDsMap;

    /**
     * 
     * @param _path
     */
    public XMLToInternalConverterRLDA(String _path) {
        super(_path);
    }

    /**
     *
     * @param _converterExperimentParameters
     */
    public XMLToInternalConverterRLDA(
          ConverterExperimentParameters _converterExperimentParameters) {
        super(_converterExperimentParameters);

        degreesPerRegion = converterExperimentParameters.getDegreesPerRegion();
        toponymToRegionIDsMap = new ToponymToRegionIDsMap();
        activeRegions = 0;
    }

    /**
     * Initialize the region array to be of the right size and contain null
     * pointers.
     */
    @Override
    public void initializeRegionArray() {
        activeRegions = 0;

        int regionArrayWidth = 360 / /*(int)*/ degreesPerRegion;
        int regionArrayHeight = 180 / /*(int)*/ degreesPerRegion;

        regionArray = new Region[regionArrayWidth][regionArrayHeight];

        for (int w = 0; w < regionArrayWidth; w++) {
            for (int h = 0; h < regionArrayHeight; h++) {
                regionArray[w][h] = null;
            }
        }
    }

    /**
     * Add a single location to the Region object in the region array that
     * corresponds to the latitude and longitude stored in the location object.
     * Create the Region object if necessary. 
     *
     * @param loc
     */
    protected Region getRegion(Coordinate coord) {
        int curX = ((int) Math.floor(coord.longitude + 180)) / degreesPerRegion;
        int curY = ((int) Math.floor(coord.latitude + 90)) / degreesPerRegion;

        if (regionArray[curX][curY] == null) {
            double minLon = coord.longitude - (coord.longitude + 180) % degreesPerRegion;
            double maxLon = minLon + degreesPerRegion;
            double minLat = coord.latitude - (coord.latitude + 90) % degreesPerRegion;
            double maxLat = minLat + degreesPerRegion;
            regionArray[curX][curY] = new Region(activeRegions, minLon, maxLon, minLat, maxLat);
            activeRegions += 1;
        }
        return regionArray[curX][curY];
    }

    /**
     * 
     */
    @Override
    public void writeToFiles() {
        OutputWriter outputWriter = null;
        switch (converterExperimentParameters.getInputFormat()) {
            case TEXT:
                outputWriter = new TextOutputWriter(converterExperimentParameters);
                break;
            case BINARY:
                outputWriter = new BinaryOutputWriter(converterExperimentParameters);
                break;
        }

        outputWriter.writeTokenArray(tokenArrayBuffer);
        outputWriter.writeToponymRegion(toponymToRegionIDsMap);
        outputWriter.writeLexicon(lexicon);
        outputWriter.writeRegions(regionArray);
    }

    @Override
    protected void addToTopoStructs(int _wordid, Coordinate _coord) {
        Region region = getRegion(_coord);

        int regid = region.id;

        if (!toponymToRegionIDsMap.containsKey(_wordid)) {
            toponymToRegionIDsMap.put(_wordid, new HashSet<Integer>());
        }

        toponymToRegionIDsMap.get(_wordid).add(regid);
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
    public void addCoordinate(double _lat, double _long) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addCandidate(double _lat, double _long) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addRepresentative(double _lat, double _long) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setCurrentToponym(String _string) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
