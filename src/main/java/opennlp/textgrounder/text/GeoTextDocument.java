package opennlp.textgrounder.text;

import java.io.*;
import java.util.*;

import opennlp.textgrounder.topo.*;

public class GeoTextDocument extends Document {

    //private String id;
    private String timestamp;
    private Coordinate coord;

    private List<Sentence<Token>> sentences;

    /*public GeoTextDocument(String id) {
	super(id);
	}*/

    public GeoTextDocument(String id, String timestamp, double lat, double lon) {
	super(id);
	this.timestamp = timestamp;
	this.coord = new Coordinate(lat, lon);
        this.sentences = new ArrayList<Sentence<Token>>();
    }

    public void addSentence(Sentence<Token> sentence) {
        sentences.add(sentence);
    }

    public Iterator<Sentence<Token>> iterator() {
        return sentences.iterator();
    }
}
