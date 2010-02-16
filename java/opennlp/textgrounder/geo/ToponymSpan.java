package opennlp.textgrounder.geo;

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