/*
 * Weighted Minimum Distance resolver. Iterative algorithm that builds on BasicMinDistResolver by incorporating corpus-level
 * prominence of various locations into toponym resolution.
 */

package opennlp.textgrounder.resolver;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.topo.*;
import java.util.*;

public class WeightedMinDistResolver extends Resolver {

    private int numIterations;
    //private Map<Long, Double> distanceCache = new HashMap<Long, Double>();
    //private int maxCoeff = Integer.MAX_VALUE;
    private DistanceTable distanceTable;

    public WeightedMinDistResolver(int numIterations) {
        super();
        this.numIterations = numIterations;
    }

    @Override
    public StoredCorpus disambiguate(StoredCorpus corpus) {

        distanceTable = new DistanceTable(corpus.getToponymTypeCount());

        Map<Location, Double> weights = initializeWeights(corpus);

        for(int i = 0; i < numIterations; i++) {
            updateWeights(corpus, weights);
        }
        
        return finalDisambiguationStep(corpus, weights);
    }

    private Map<Location, Double> initializeWeights(StoredCorpus corpus) {
        Map<Location, Double> weights = new HashMap<Location, Double>();

        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        double uniformWeight = 1.0/toponym.getAmbiguity();
                        for(Location candidate : toponym) {
                            weights.put(candidate, uniformWeight);
                        }
                    }
                }
            }
        }

        return weights;
    }

    private void updateWeights(StoredCorpus corpus, Map<Location, Double> weights) {
        
    }

  /* This implementation of disambiguate immediately stops computing distance
   * totals for candidates when it becomes clear that they aren't minimal. */
  private StoredCorpus finalDisambiguationStep(StoredCorpus corpus, Map<Location, Double> weights) {
    for (Document<StoredToken> doc : corpus) {
      for (Sentence<StoredToken> sent : doc) {
        for (Toponym toponym : sent.getToponyms()) {
          double min = Double.MAX_VALUE;
          int minIdx = -1;

          int idx = 0;
          for (Location candidate : toponym) {
            Double candidateMin = this.checkCandidate(toponym, candidate, idx, doc, min, weights);
            if (candidateMin != null) {
              min = candidateMin;
              minIdx = idx;
            }
            idx++;
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
          double currentMinTotal, Map<Location, Double> weights) {
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

    /* The previous implementation of disambiguate. */
    public StoredCorpus disambiguateOld(StoredCorpus corpus) {
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
        int curLocIndex = 0;
        for(Location curLoc : toponymToDisambiguate) {
            Double totalDistSoFar = 0.0;
            int seen = 0;

            for(Sentence<StoredToken> sent : doc) {
                for(Token token : sent.getToponyms()) {
                    //if(token.isToponym()) {
                        Toponym otherToponym = (Toponym) token;

                        /* We don't want to compute distances if this other toponym is the
                         * same as the current one, or if it has no candidates. */
                        if (!otherToponym.equals(toponymToDisambiguate) && otherToponym.getAmbiguity() > 0) {
                          StoredToponym storedToponymToDisambiguate = (StoredToponym) toponymToDisambiguate;
                          double minDist = Double.MAX_VALUE;
                          int otherLocIndex = 0;
                          for(Location otherLoc : otherToponym) {
                              double curDist = distanceTable.getDistance(storedToponymToDisambiguate, curLocIndex, (StoredToponym)otherToponym, otherLocIndex);//curLoc.distance(otherLoc);
                              if(curDist < minDist) {
                                  minDist = curDist;
                              }
                              otherLocIndex++;
                          }
                          totalDistSoFar += minDist;
                          seen++;
                        }
                    //}
                }
            }

            /* Abstain if we haven't seen any other toponyms. */
            totalDistances.add(seen > 0 ? totalDistSoFar : Double.MAX_VALUE);
            curLocIndex++;
        }

        // Find the overall minimum of all the total minimum distances computed above
        double minTotalDist = Double.MAX_VALUE;
        int indexOfMin = -1;
        for(curLocIndex = 0; curLocIndex < totalDistances.size(); curLocIndex++) {
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
        private double[][][][] allDistances;

        public DistanceTable(int numToponymTypes) {
            allDistances = new double[numToponymTypes][numToponymTypes][][];
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

