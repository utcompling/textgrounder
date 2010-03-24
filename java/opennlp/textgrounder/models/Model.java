///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 3 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.models;

import opennlp.textgrounder.io.DocumentSet;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import java.util.ArrayList;

import java.util.Hashtable;
import java.util.List;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.ners.SNERPairListSet;
import opennlp.textgrounder.ners.ToponymSpan;
import opennlp.textgrounder.topostructs.*;

/**
 * Base abstract class for all training models. Defines some methods for
 * interfacing with gazetteers. Declares some fields
 * 
 * @author tsmoon
 */
public abstract class Model {

    protected Gazetteer gazetteer;
    protected SNERPairListSet pairListSet;
    protected Hashtable<String, List<Location>> gazCache;
    protected double degreesPerRegion;
    protected Region[][] regionArray;
    protected int activeRegions;
    /**
     * Collection of training data
     */
    protected DocumentSet docSet;

    protected String stripPunc(String aString) {
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(0))) {
            aString = aString.substring(1);
        }
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(aString.length() - 1))) {
            aString = aString.substring(0, aString.length() - 1);
        }
        return aString;
    }

    protected String getPlacenameString(ToponymSpan curTopSpan, int docIndex) {
        String toReturn = "";
        ArrayList<Integer> curDoc = docSet.get(docIndex);

        for (int i = curTopSpan.begin; i < curTopSpan.end; i++) {
            toReturn += docSet.getWordForInt(curDoc.get(i)) + " ";
        }

        return stripPunc(toReturn.trim());
    }

    protected void addLocationsToRegionArray(List<Location> locs) {
        for (Location loc : locs) {
            int curX = (int) (loc.coord.latitude + 180) / (int) degreesPerRegion;
            int curY = (int) (loc.coord.longitude + 90) / (int) degreesPerRegion;
            if (regionArray[curX][curY] == null) {
                double minLon = loc.coord.longitude - loc.coord.longitude % degreesPerRegion;
                double maxLon = minLon + degreesPerRegion;
                double minLat = loc.coord.latitude - loc.coord.latitude % degreesPerRegion;
                double maxLat = minLat + degreesPerRegion;
                regionArray[curX][curY] = new Region(minLon, maxLon, minLat, maxLat);
                activeRegions++;
            }
        }
    }
}
