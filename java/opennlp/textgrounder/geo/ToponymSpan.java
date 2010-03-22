package opennlp.textgrounder.geo;

/**
 * Class for handling toponyms in a natural language text. Pair object that
 * contains a beginning index and an ending index for a given toponym. The indexes
 * are given in terms of words. At the moment, toponyms are identified by
 * the Stanford NER classifier and ToponymSpan are populated and tabulated in
 * {@link SNERPairListSet#addToponymSpansFromFile(java.lang.String)}.
 *
 * @author
 */
public class ToponymSpan {

    public int begin;
    public int end;

    public ToponymSpan(int b, int e) {
        begin = b;
        end = e;
    }

    public int getLength() {
        return end - begin;
    }
}
