/*
 * Weighted Minimum Distance resolver. Iterative algorithm that builds on BasicMinDistResolver by incorporating corpus-level
 * prominence of various locations into toponym resolution.
 */

package opennlp.textgrounder.tr.resolver;

import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.topo.*;
import opennlp.textgrounder.tr.util.*;
import java.util.*;
import java.io.*;

public class WeightedMinDistResolver extends Resolver {

    // weights and toponym lexicon (for indexing into weights) are stored so that a different
    //   corpus/corpora can be used for training than for disambiguating
    private List<List<Double> > weights = null;
    Lexicon<String> toponymLexicon = null;

    private int numIterations;
    private boolean readWeightsFromFile;
    private String logFilePath;
    private List<List<Double> > weightsFromFile = null;
    //private Map<Long, Double> distanceCache = new HashMap<Long, Double>();
    //private int maxCoeff = Integer.MAX_VALUE;
    private DistanceTable distanceTable;
    private static final int PHANTOM_COUNT = 0; // phantom/imagined counts for smoothing

    public WeightedMinDistResolver(int numIterations, boolean readWeightsFromFile, String logFilePath) {
        super();
        this.numIterations = numIterations;
        this.readWeightsFromFile = readWeightsFromFile;
        this.logFilePath = logFilePath;

        if(readWeightsFromFile && logFilePath == null) {
            System.err.println("Error: need logFilePath via -l for backoff to DocDist.");
            System.exit(0);
        }
    }

    public WeightedMinDistResolver(int numIterations, boolean readWeightsFromFile) {
        this(numIterations, readWeightsFromFile, null);
    }

    @Override
    public void train(StoredCorpus corpus) {

        distanceTable = new DistanceTable(corpus.getToponymTypeCount());

        toponymLexicon = TopoUtil.buildLexicon(corpus);
        List<List<Integer> > counts = new ArrayList<List<Integer> >(toponymLexicon.size());
        for(int i = 0; i < toponymLexicon.size(); i++) counts.add(null);
        weights = new ArrayList<List<Double> >(toponymLexicon.size());
        for(int i = 0; i < toponymLexicon.size(); i++) weights.add(null);

        if(readWeightsFromFile) {
            weightsFromFile = new ArrayList<List<Double> >(toponymLexicon.size());
            try {
                DataInputStream in = new DataInputStream(new FileInputStream("probToWMD.dat"));
                for(int i = 0; i < toponymLexicon.size(); i++) {
                    int ambiguity = in.readInt();
                    weightsFromFile.add(new ArrayList<Double>(ambiguity));
                    for(int j = 0; j < ambiguity; j++) {
                        weightsFromFile.get(i).add(in.readDouble());
                    }
                }
                in.close();
            } catch(Exception e) {
                e.printStackTrace();
                System.exit(1);
            }

            /*for(int i = 0; i < weightsFromFile.size(); i++) {
                for(int j = 0; j < weightsFromFile.get(i).size(); j++) {
                    System.out.println(weightsFromFile.get(i).get(j));
                }
                System.out.println();
                }*/
        }

        initializeCountsAndWeights(counts, weights, corpus, toponymLexicon, PHANTOM_COUNT, weightsFromFile);

        for(int i = 0; i < numIterations; i++) {
            System.out.println("Iteration: " + (i+1));
            updateWeights(corpus, counts, PHANTOM_COUNT, weights, toponymLexicon);
        }
    }

    @Override
    public StoredCorpus disambiguate(StoredCorpus corpus) {

        if(weights == null)
            train(corpus);

        TopoUtil.addToponymsToLexicon(toponymLexicon, corpus);
        weights = expandWeightsArray(toponymLexicon, corpus, weights);
        
        StoredCorpus disambiguated = finalDisambiguationStep(corpus, weights, toponymLexicon);

        if(readWeightsFromFile) {
            // Backoff to DocDist:
            Resolver docDistResolver = new DocDistResolver(logFilePath);
            docDistResolver.overwriteSelecteds = false;
            disambiguated = docDistResolver.disambiguate(corpus);
        }
        else {
            // Backoff to Random:
            Resolver randResolver = new RandomResolver();
            randResolver.overwriteSelecteds = false;
            disambiguated = randResolver.disambiguate(corpus);
        }

        return disambiguated;
    }

    // adds a weight of 1.0 to candidate locations of toponyms found in lexicon but not in oldWeights
    private List<List<Double> > expandWeightsArray(Lexicon<String> lexicon, StoredCorpus corpus, List<List<Double> > oldWeights) {
        if(oldWeights.size() >= lexicon.size())
            return oldWeights;
        
        List<List<Double> > newWeights = new ArrayList<List<Double> >(lexicon.size());
        for(int i = 0; i < lexicon.size(); i++) newWeights.add(null);

        for(int i = 0; i < oldWeights.size(); i++) {
            newWeights.set(i, oldWeights.get(i));
        }

        initializeWeights(newWeights, corpus, lexicon);

        return newWeights;
    }

    private void initializeCountsAndWeights(List<List<Integer> > counts, List<List<Double> > weights,
                                            StoredCorpus corpus, Lexicon<String> lexicon, int initialCount,
                                            List<List<Double> > weightsFromFile) {

        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int index = lexicon.get(toponym.getForm());
                        if(counts.get(index) == null) {
                            counts.set(index, new ArrayList<Integer>(toponym.getAmbiguity()));
                            weights.set(index, new ArrayList<Double>(toponym.getAmbiguity()));
                            for(int i = 0; i < toponym.getAmbiguity(); i++) {
                                counts.get(index).add(initialCount);
                                if(weightsFromFile != null
                                   && weightsFromFile.get(index).size() > 0)
                                    weights.get(index).add(weightsFromFile.get(index).get(i));
                                else
                                    weights.get(index).add(1.0);
                            }
                        }
                    }
                }
            }
        }
    }

    private void initializeWeights(List<List<Double> > weights, StoredCorpus corpus, Lexicon<String> lexicon) {
        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int index = lexicon.get(toponym.getForm());
                        if(weights.get(index) == null) {
                            weights.set(index, new ArrayList<Double>(toponym.getAmbiguity()));
                            for(int i = 0; i < toponym.getAmbiguity(); i++) {
                                weights.get(index).add(1.0);
                            }
                        }
                    }
                }
            }       
        }
    }

    private void updateWeights(StoredCorpus corpus, List<List<Integer> > counts, int initialCount, List<List<Double> > weights, Lexicon<String> lexicon) {
        
        for(int i = 0; i < counts.size(); i++)
            for(int j = 0; j < counts.get(i).size(); j++)
                counts.get(i).set(j, initialCount);

        List<Integer> sums = new ArrayList<Integer>(counts.size());
        for(int i = 0; i < counts.size(); i++) sums.add(initialCount * counts.get(i).size());

        for (Document<StoredToken> doc : corpus) {
            for (Sentence<StoredToken> sent : doc) {
                for (Toponym toponym : sent.getToponyms()) {
                    double min = Double.MAX_VALUE;
                    int minIdx = -1;
                    
                    int idx = 0;
                    for (Location candidate : toponym) {
                        Double candidateMin = this.checkCandidate(toponym, candidate, idx, doc, min, weights, lexicon);
                        if (candidateMin != null) {
                            min = candidateMin;
                            minIdx = idx;
                        }
                        idx++;
                    }

                    if(minIdx == -1) { // Most likely happens when there was only one toponym in the document;
                                       //   so, choose the location with the greatest weight (unless all are uniform)
                        double maxWeight = 1.0;
                        int locationIdx = 0;
                        for(Location candidate : toponym) {
                            double thisWeight = weights.get(lexicon.get(toponym.getForm())).get(locationIdx);
                            if(thisWeight > maxWeight) {
                                maxWeight = thisWeight;
                                minIdx = locationIdx;
                            }
                        }
                        locationIdx++;
                    }
                    
                    if (minIdx > -1) {
                        int countIndex = lexicon.get(toponym.getForm());
                        int prevCount = counts.get(countIndex)
                            .get(minIdx);
                        counts.get(countIndex).set(minIdx, prevCount + 1);
                        int prevSum = sums.get(countIndex);
                        sums.set(countIndex, prevSum + 1);
                        
                    }

                }
            }
        }
    
    
        for(int i = 0; i < weights.size(); i++) {
            List<Double> curWeights = weights.get(i);
            List<Integer> curCounts = counts.get(i);
            int curSum = sums.get(i);
            for(int j = 0; j < curWeights.size(); j++) {
                curWeights.set(j, ((double)curCounts.get(j) / curSum) * curWeights.size());
            }
        }
    }


  /* This implementation of disambiguate immediately stops computing distance
   * totals for candidates when it becomes clear that they aren't minimal. */
  private StoredCorpus finalDisambiguationStep(StoredCorpus corpus, List<List<Double> > weights, Lexicon<String> lexicon) {
    for (Document<StoredToken> doc : corpus) {
        for (Sentence<StoredToken> sent : doc) {
            for (Toponym toponym : sent.getToponyms()) {
                double min = Double.MAX_VALUE;
                int minIdx = -1;
                
                int idx = 0;
                for (Location candidate : toponym) {
                    Double candidateMin = this.checkCandidate(toponym, candidate, idx, doc, min, weights, lexicon);
                    if (candidateMin != null) {
                        min = candidateMin;
                        minIdx = idx;
                    }
                    idx++;
                }

                if(minIdx == -1) { // Most likely happens when there was only one toponym in the document;
                                   //   so, choose the location with the greatest weight (unless all are 1.0)
                    double maxWeight = 1.0;
                    int locationIdx = 0;
                    for(Location candidate : toponym) {
                        double thisWeight = weights.get(lexicon.get(toponym.getForm())).get(locationIdx);
                        if(thisWeight > maxWeight) {
                            maxWeight = thisWeight;
                            minIdx = locationIdx;
                        }
                    }
                    locationIdx++;
                }
                
                if (minIdx > -1) {
                    toponym.setSelectedIdx(minIdx);
                }
            }
        }
        
    }
    
    return corpus;
  }

  /* Returns the minimum total distance to all other locations in the document
   * for the candidate, or null if it's greater than the current minimum. */
  private Double checkCandidate(Toponym toponymTemp, Location candidate, int locationIndex, Document<StoredToken> doc,
          double currentMinTotal, List<List<Double> > weights, Lexicon<String> lexicon) {
    StoredToponym toponym = (StoredToponym) toponymTemp;
    Double total = 0.0;
    int seen = 0;

    for (Sentence<StoredToken> otherSent : doc) {
      for (Toponym otherToponymTemp : otherSent.getToponyms()) {
        StoredToponym otherToponym = (StoredToponym) otherToponymTemp;

        /*Map<Location, Double> normalizationDenoms = new HashMap<Location, Double>();
        for(Location tempOtherLoc : otherToponym) {//int i = 0; i < otherToponym.getAmbiguity(); i++) {
            //Location tempOtherLoc = otherToponym.getCandidates().get(i);
            double normalizationDenomTemp = 0.0;
            for(Location tempLoc : toponym) { //int j = 0; j < toponym.getAmbiguity(); j++) {
                //Location tempLoc = toponym.getCandidates().get(j);
                normalizationDenomTemp += tempOtherLoc.distance(tempLoc);
            }
            normalizationDenoms.put(tempOtherLoc, normalizationDenomTemp);
        }*/

        /* We don't want to compute distances if this other toponym is the
         * same as the current one, or if it has no candidates. */
        if (!otherToponym.equals(toponym) && otherToponym.getAmbiguity() > 0) {
          double min = Double.MAX_VALUE;
          //double sum = 0.0;

          int otherLocIndex = 0;
          for (Location otherLoc : otherToponym) {

            /*double normalizationDenom = 0.0;
            for(Location tempCand : toponym) {
              normalizationDenom += tempCand.distance(otherLoc)  / weights.get(otherLoc) ;
            }
            */
            //double normalizationDenom = normalizationDenoms.get(otherToponym);

            double weightedDist = distanceTable.getDistance(toponym, locationIndex, otherToponym, otherLocIndex);//candidate.distance(otherLoc) /* / weights.get(otherLoc) */ ;
            double thisWeight = weights.get(lexicon.get(toponym.getForm())).get(locationIndex);
            double otherWeight = weights.get(lexicon.get(otherToponym.getForm())).get(otherLocIndex);
            weightedDist /= (thisWeight * otherWeight); // weighting; was just otherWeight before
            //weightedDist /= normalizationDenoms.get(otherLoc); // normalization
            if (weightedDist < min) {
              min = weightedDist;
            }
            //sum += weightedDist;
            otherLocIndex++;
          }

          seen++;
          total += min;
          //total += sum;

          /* If the running total is greater than the current minimum, we can
           * stop. */
          if (total >= currentMinTotal) {
            return null;
          }
        }
      }
    }

    /* Abstain if we haven't seen any other toponyms. */
    return seen > 0 ? total : null;
  }

    /*private double getDistance(StoredToponym t1, int i1, StoredToponym t2, int i2) {
        int t1idx = t1.getIdx();
        int t2idx = t2.getIdx();

        long key = t1idx + i1 * maxCoeff + t2idx * maxCoeff * maxCoeff + i2 * maxCoeff * maxCoeff * maxCoeff;

        Double dist = distanceCache.get(key);

        if(dist == null) {
            dist = t1.getCandidates().get(i1).distance(t2.getCandidates().get(i2));
            distanceCache.put(key, dist);
            long key2 = t2idx + i2 * maxCoeff + t1idx * maxCoeff * maxCoeff + i1 * maxCoeff * maxCoeff * maxCoeff;
            distanceCache.put(key2, dist);
        }

        return dist;
    }*/

    private class DistanceTable {
        //private double[][][][] allDistances;

        public DistanceTable(int numToponymTypes) {
            //allDistances = new double[numToponymTypes][numToponymTypes][][];
        }

        public double getDistance(StoredToponym t1, int i1, StoredToponym t2, int i2) {

            return t1.getCandidates().get(i1).distance(t2.getCandidates().get(i2));
            
            /*int t1idx = t1.getIdx();
            int t2idx = t2.getIdx();

            double[][] distanceMatrix = allDistances[t1idx][t2idx];
            if(distanceMatrix == null) {
                distanceMatrix = new double[t1.getAmbiguity()][t2.getAmbiguity()];
                for(int i = 0; i < distanceMatrix.length; i++) {
                    for(int j = 0; j < distanceMatrix[i].length; j++) {
                        distanceMatrix[i][j] = t1.getCandidates().get(i).distance(t2.getCandidates().get(j));
                    }
                }
            }*/
            /*double distance = distanceMatrix[i1][i2];
            if(distance == 0.0) {
                distance = t1.getCandidates().get(i1).distance(t2.getCandidates().get(i2));
                distanceMatrix[i1][i2] = distance;
            }*/
            //return distanceMatrix[i1][i2];
            
        }

        //public
    }
}
