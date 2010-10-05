/*
 * This is a simple Evaluator that assumes gold named entities were used in preprocessing. For each gold disambiguated toponym, the model
 * either got that Location right or wrong, and a Report containing the accuracy figure on this task is returned.
 */

package opennlp.textgrounder.eval;

import opennlp.textgrounder.text.*;

public class AccuracyEvaluator extends Evaluator {

    public AccuracyEvaluator(Corpus corpus) {
        super(corpus);
    }

    public Report evaluate() {

        Report report = new Report();

        for(Document<Token> doc : corpus) {
            for(Sentence<Token> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.hasGold()) {
                        if(toponym.getGoldIdx() == toponym.getSelectedIdx()) {
                            report.incrementTP();
                        }
                        else {
                            report.incrementInstanceCount();
                        }
                    }
                }
            }
        }

        return report;
    }
    
    public Report evaluate(Corpus<Token> pred, boolean useSelected) {
        return null;
    }
}
