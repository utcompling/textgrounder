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
    public Corpus<Token> disambiguate(Corpus<Token> corpus) {
    /*    for(Document doc : corpus) {
            for(Sentence sent : doc) {
                for(Token token : sent) {
                    if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;

                        basicMinDistDisambiguate(toponym, doc);
                    }
                }
            }
        }*/
        return corpus;
    }

    /*
     * Sets the selected index of toponymToDisambiguate according to the Location with the minimum total
     * distance to some disambiguation of all the Locations of the Toponyms in doc.
     */
    private void basicMinDistDisambiguate(Toponym toponymToDisambiguate, Document doc) {
        HashMap<Location, Double> totalDistances = new HashMap<Location, Double>();

        // Compute the total minimum distances from each candidate Location of toponymToDisambiguate to some disambiguation
        // of all the Toponyms in doc; store these in totalDistances
        /*for(Location curLoc : toponymToDisambiguate) {
            for(Sentence sent : doc) {
                for(Token token : sent) {
                    if(token.isToponym()) {
                        Toponym otherToponym = (Toponym) token;

                        double minDist = Double.MAX_VALUE;
                        for(Location otherLoc : otherToponym) {
                            double curDist = curLoc.distance(otherLoc);
                            if(curDist < minDist) {
                                minDist = curDist;
                            }
                        }
                        Double totalDistSoFar = totalDistances.get(curLoc);
                        if(totalDistSoFar == null)
                            totalDistSoFar = 0.0;
                        totalDistances.put(curLoc, totalDistSoFar + minDist);
                    }
                }
            }
        }*/

        // Find the overall minimum of all the total minimum distances computed above
        double minTotalDist = Double.MAX_VALUE;
        int indexOfMin = -1;
        int curLocIndex = -1;
        for(Location curLoc : toponymToDisambiguate) {
            curLocIndex++;
            double totalDist = totalDistances.get(curLoc);
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
