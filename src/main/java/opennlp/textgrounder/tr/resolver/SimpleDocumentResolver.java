/*
 * This resolves each Document (which currently must be a GeoTextDocument) to a particular coordinate given that its toponyms have already
 * been resolved.
 */

package opennlp.textgrounder.tr.resolver;

import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.topo.*;
import java.util.*;

public class SimpleDocumentResolver extends Resolver {

    public StoredCorpus disambiguate(StoredCorpus corpus) {
        return disambiguate(corpus, null);
    }

    public StoredCorpus disambiguate(StoredCorpus corpus, Region boundingBox) {

        for(Document<StoredToken> doc : corpus) {

            Map<Integer, Integer> locationCounts = new HashMap<Integer, Integer>();
            int greatestLocFreq = 0;
            Location mostCommonLoc = null;

            /*if(doc instanceof GeoTextDocument) {
                System.out.println("doc " + doc.getId() + " is a GeoTextDocument.");
            }
            else
                System.out.println("doc " + doc.getId() + " is NOT a GeoTextDocument; it's a " + doc.getClass().getName());
            */

            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if (toponym.hasSelected()) {
                        Location systemLoc = toponym.getCandidates().get(toponym.getSelectedIdx());
                        if(systemLoc.getRegion().getRepresentatives().size() == 1
                           && (boundingBox == null || boundingBox.contains(systemLoc.getRegion().getCenter()))) {
                            int locId = systemLoc.getId();
                            Integer prevCount = locationCounts.get(locId);
                            if(prevCount == null)
                                prevCount = 0;
                            locationCounts.put(locId, prevCount + 1);
                            
                            if(prevCount + 1 > greatestLocFreq) {
                                greatestLocFreq = prevCount + 1;
                                mostCommonLoc = systemLoc;
                                //System.out.println(mostCommonLoc.getName() + " is now most common with " + (prevCount+1));
                            }
                        }
                    }
                }
            }
            
            if(mostCommonLoc != null) {
                doc.setSystemCoord(mostCommonLoc.getRegion().getCenter());
                //System.out.println("Setting mostCommonLoc for " + doc.getId() + " to " + mostCommonLoc.getName());
                //System.out.println("goldCoord was " + doc.getGoldCoord());
            }
            else if(boundingBox != null) {
                doc.setSystemCoord(boundingBox.getCenter());
            }
            else {
                doc.setSystemCoord(Coordinate.fromDegrees(0.0, 0.0));
            }
        }

        return corpus;
    }    
}
