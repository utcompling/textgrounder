///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
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
package opennlp.textgrounder.models;

import gnu.trove.TIntHashSet;
import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.CommandLineOptions;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.textstructs.*;

/**
 *
 * @author tsmoon
 */
public abstract class SelfTrainedModelBase extends Model {

    /**
     * Gazetteer that holds geographic information
     */
    protected Gazetteer gazetteer;

    public SelfTrainedModelBase(Gazetteer gaz, int bscale, int paragraphsAsDocs) {
        barScale = bscale;
        gazetteer = gaz;
        gazetteer.gazetteerRefresh = gazetteerRefresh;
        lexicon = new Lexicon();
    }

    public SelfTrainedModelBase(CommandLineOptions options) {
        super(options);
        gazetteer = gazetteerGenerator.generateGazetteer();
        gazetteer.gazetteerRefresh = gazetteerRefresh;
    }

    public void activateRegionsForWholeGaz() throws Exception {
        System.out.println("Running whole gazetteer through system...");

        //locations = new ArrayList<Location>();
        locations = gazetteer.getAllLocalities();
        addLocationsToRegionArray(locations);
        //locations = null; //////////////// uncomment this to get a null pointer but much faster termination
        //                                   if you only want to know the number of active regions :)
    }

    /**
     * Add locations to the 2D regionArray. To be used by classes that
     * do not use a RegionMapperCallback.
     *
     * @param locs list of locations.
     */
    protected void addLocationsToRegionArray(TIntHashSet locs) {
        addLocationsToRegionArray(locs, gazetteer, new NullRegionMapperCallback());
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     *
     * @throws Exception
     */
    public void writeXMLFile() throws Exception {
        if (!runWholeGazetteer) {
            writeXMLFile(trainInputPath, kmlOutputFilename, gazetteer.getIdxToLocationMap(),locations, evalTokenArrayBuffer);
        } else {
            writeXMLFile("WHOLE_GAZETTEER", kmlOutputFilename, gazetteer.getIdxToLocationMap(), locations, evalTokenArrayBuffer);
        }
    }
}
