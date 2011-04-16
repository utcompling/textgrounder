package opennlp.textgrounder.eval;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.topo.*;

public class DocDistanceEvaluator {

    protected final Corpus<Token> corpus;

    public DocDistanceEvaluator(Corpus<? extends Token> corpus) {
        this.corpus = (Corpus<Token>)corpus;
    }

  /* Evaluate the "selected" candidates in the corpus using its "gold"
   * candidates. */
    public DistanceReport evaluate() {
        DistanceReport dreport = new DistanceReport();

        for(Document<Token> doc : corpus) {

            Coordinate systemCoord = doc.getSystemCoord();
            Coordinate goldCoord = doc.getGoldCoord();
            /*if(systemCoord == null)
                System.out.println("systemCoord null for docid " + doc.getId());
            if(goldCoord == null)
            System.out.println("goldCoord null for docid " + doc.getId());*/
            //if(systemCoord != null && goldCoord == null)
            //    System.out.println("have system coord but no gold coord for " + doc.getId());
            if(systemCoord != null && goldCoord != null) {
                dreport.addDistance(systemCoord.distanceInKm(goldCoord));
            }
        }

        return dreport;
    }

}
