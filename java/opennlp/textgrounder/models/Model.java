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
 * interfacing with gazetteers. Declares some fields that deal with regions
 * and placenames.
 * 
 * @author 
 */
public abstract class Model {

    /**
     *
     */
    protected Gazetteer gazetteer;
    /**
     *
     */
    protected SNERPairListSet pairListSet;
    /**
     *
     */
    protected Hashtable<String, List<Location>> gazCache;
    /**
     *
     */
    protected double degreesPerRegion;
    /**
     *
     */
    protected Region[][] regionArray;
    /**
     * 
     */
    protected int activeRegions;
    /**
     * Collection of training data
     */
    protected DocumentSet docSet;

    /**
     *
     * @param aString
     * @return
     */
    protected String stripPunc(String aString) {
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(0))) {
            aString = aString.substring(1);
        }
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(aString.length() - 1))) {
            aString = aString.substring(0, aString.length() - 1);
        }
        return aString;
    }

    /**
     * 
     * @param curTopSpan
     * @param docIndex
     * @return
     */
    protected String getPlacenameString(ToponymSpan curTopSpan, int docIndex) {
        String toReturn = "";
        ArrayList<Integer> curDoc = docSet.get(docIndex);

        for (int i = curTopSpan.begin; i < curTopSpan.end; i++) {
            toReturn += docSet.getWordForInt(curDoc.get(i)) + " ";
        }

        return stripPunc(toReturn.trim());
    }

    /**
     * 
     * @param locs
     */
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
