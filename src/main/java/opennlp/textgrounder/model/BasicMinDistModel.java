/*
 * Basic Minimum Distance model. For each toponym, the location is selected that minimizes the total distance to some disambiguation
 * of the other toponyms in the same document.
 */

package opennlp.textgrounder.model;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.topo.*;
import java.util.*;

public class BasicMinDistModel extends Model {

    @Override
    public StoredCorpus disambiguate(StoredCorpus corpus) {
        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Token token : sent.getToponyms()) {
                    //if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;

                        basicMinDistDisambiguate(toponym, doc);
                    //}
                }
            }
        }
        return corpus;
    }

    /*
     * Sets the selected index of toponymToDisambiguate according to the Location with the minimum total
     * distance to some disambiguation of all the Locations of the Toponyms in doc.
     */
    private void basicMinDistDisambiguate(Toponym toponymToDisambiguate, Document<StoredToken> doc) {        
        //HashMap<Location, Double> totalDistances = new HashMap<Location, Double>();
        List<Double> totalDistances = new ArrayList<Double>();

        // Compute the total minimum distances from each candidate Location of toponymToDisambiguate to some disambiguation
        // of all the Toponyms in doc; store these in totalDistances
        for(Location curLoc : toponymToDisambiguate) {
            Double totalDistSoFar = 0.0;
            for(Sentence<StoredToken> sent : doc) {
                for(Token token : sent.getToponyms()) {
                    //if(token.isToponym()) {
                        Toponym otherToponym = (Toponym) token;

                        double minDist = Double.MAX_VALUE;
                        for(Location otherLoc : otherToponym) {
                            double curDist = curLoc.distance(otherLoc);
                            if(curDist < minDist) {
                                minDist = curDist;
                            }
                        }
                        totalDistSoFar += minDist;
                    //}
                }
            }
            totalDistances.add(totalDistSoFar);
        }

        // Find the overall minimum of all the total minimum distances computed above
        double minTotalDist = Double.MAX_VALUE;
        int indexOfMin = -1;
        for(int curLocIndex = 0; curLocIndex < totalDistances.size(); curLocIndex++) {
            double totalDist = totalDistances.get(curLocIndex);
            if(totalDist < minTotalDist) {
                minTotalDist = totalDist;
                indexOfMin = curLocIndex;
            }
        }

        // Set toponymToDisambiguate's index to the index of the Location with the overall minimum distance
        // from above, if one was found
        if(indexOfMin >= 0) {
            toponymToDisambiguate.setSelectedIdx(indexOfMin);
        }
    }
}
