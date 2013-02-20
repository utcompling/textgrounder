/*
 * Evaluator that uses signatures around each gold and predicted toponym to be used in the computation of P/R/F.
 */

package opennlp.textgrounder.tr.eval;

import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.topo.*;
import java.util.*;
import java.io.*;

public class SignatureEvaluator extends Evaluator {

    private static final int CONTEXT_WINDOW_SIZE = 20;

    private static final double FP_PENALTY = 20037.5;
    private static final double FN_PENALTY = 20037.5;

    private Map<String, List<Location> > predCandidates = new HashMap<String, List<Location> >();

    public SignatureEvaluator(Corpus goldCorpus) {
        super(goldCorpus);
    }

    public Report evaluate() {
        return null;
    }

    private Map<String, Location> populateSigsAndLocations(Corpus<Token> corpus, boolean getGoldLocations) {
        Map<String, Location> locs = new HashMap<String, Location>();

        for(Document<Token> doc : corpus) {
            //System.out.println("Document id: " + doc.getId());
            for(Sentence<Token> sent : doc) {
                StringBuffer sb = new StringBuffer();
                List<Integer> toponymStarts = new ArrayList<Integer>();
                List<Location> curLocations = new ArrayList<Location>();
                List<List<Location> > curCandidates = new ArrayList<List<Location> >();
                for(Token token : sent) {
                    //System.out.println(token.getForm());
                    if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;
                        if((getGoldLocations && toponym.hasGold()) ||
                           (!getGoldLocations && (toponym.hasSelected() || toponym.getAmbiguity() == 0))) {
                            toponymStarts.add(sb.length());
                            if(getGoldLocations) {
				/*if(toponym.getGoldIdx() == 801) {
				    System.out.println(toponym.getForm()+": "+toponym.getGoldIdx()+"/"+toponym.getCandidates().size());
                                    }*/
                                curLocations.add(toponym.getCandidates().get(toponym.getGoldIdx()<toponym.getCandidates().size()?toponym.getGoldIdx():toponym.getCandidates().size()-1));
			    }
                            else {
                                if(toponym.getAmbiguity() > 0)
                                    curLocations.add(toponym.getCandidates().get(toponym.getSelectedIdx()));
                                else
                                    curLocations.add(null);
                                curCandidates.add(toponym.getCandidates());
                            }
                        }
                    }
                    sb.append(token.getForm().replaceAll("[^a-z0-9]", ""));
                }
                for(int i = 0; i < toponymStarts.size(); i++) {
                    int toponymStart = toponymStarts.get(i);
                    Location curLoc = curLocations.get(i);
                    String context = getSignature(sb, toponymStart, CONTEXT_WINDOW_SIZE);
                    locs.put(context, curLoc);
                    if(!getGoldLocations)
                        predCandidates.put(context, curCandidates.get(i));
                }
            }
        }

        return locs;
    }

    private DistanceReport dreport = null;
    public DistanceReport getDistanceReport() { return dreport; }

    @Override
    public Report evaluate(Corpus<Token> pred, boolean useSelected) {
        
        Report report = new Report();
        dreport = new DistanceReport();

        Map<String, Location> goldLocs = populateSigsAndLocations(corpus, true);
        Map<String, Location> predLocs = populateSigsAndLocations(pred, false);

        Map<String, List<Double> > errors = new HashMap<String, List<Double> >();

        for(String context : goldLocs.keySet()) {
            if(predLocs.containsKey(context)) {
                Location goldLoc = goldLocs.get(context);
                Location predLoc = predLocs.get(context);

                if(predLoc != null) {
                    double dist = goldLoc.distanceInKm(predLoc);
                    dreport.addDistance(dist);
                    if(!errors.containsKey(goldLoc.getName().toLowerCase()))
                        errors.put(goldLoc.getName().toLowerCase(), new ArrayList<Double>());
                    errors.get(goldLoc.getName().toLowerCase()).add(dist);
                }

                if(isClosestMatch(goldLoc, predLoc, predCandidates.get(context))) {//goldLocs.get(context) == predLocs.get(context)) {
                    //System.out.println("TP: " + context + "|" + goldLocs.get(context));
                    report.incrementTP();
                }
                else {
                    //System.out.println("FP and FN: " + context + "|" + goldLocs.get(context) + " vs. " + predLocs.get(context));
                    //report.incrementFP();
                    //report.incrementFN();
                    report.incrementFPandFN();
                }
            }
            else {
                //System.out.println("FN: " + context + "| not found in pred");
                report.incrementFN();
                //dreport.addDistance(FN_PENALTY);
                
            }
        }
        for(String context : predLocs.keySet()) {
            if(!goldLocs.containsKey(context)) {
                //System.out.println("FP: " + context + "| not found in gold");
                report.incrementFP();
                //dreport.addDistance(FP_PENALTY);
            }
        }

        try {
        BufferedWriter errOut = new BufferedWriter(new FileWriter("errors.txt"));

        for(String toponym : errors.keySet()) {
            List<Double> errorList = errors.get(toponym);
            double sum = 0.0;
            for(double error : errorList) {
                sum += error;
            }
            errOut.write(toponym+" "+sum+" "+errorList.size()+"\n");
        }

        errOut.close();

        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return report;
    }

    private boolean isClosestMatch(Location goldLoc, Location predLoc, List<Location> curPredCandidates) {
        if(predLoc == null)
            return false;

        double distanceToBeat = predLoc.distance(goldLoc);

        for(Location otherLoc : curPredCandidates) {
            if(otherLoc.distance(goldLoc) < distanceToBeat)
                return false;
        }
        return true;
    }

    private String getSignature(StringBuffer wholeContext, int centerIndex, int windowSize) {
        int beginIndex = Math.max(0, centerIndex - windowSize);
        int endIndex = Math.min(wholeContext.length(), centerIndex + windowSize);

        return wholeContext.substring(beginIndex, endIndex);
    }
}
