package opennlp.textgrounder.text;

import java.io.*;
import java.util.*;

import opennlp.textgrounder.topo.*;

public class GeoTextDocument extends Document {

    private List<Sentence<Token>> sentences;

    public GeoTextDocument(String id, String timestamp, double goldLat, double goldLon) {
	super(id);
	this.timestamp = timestamp;
	this.goldCoord = Coordinate.fromDegrees(goldLat, goldLon);
        //System.out.println("set gold coord for " + this.id + " to " + this.goldCoord.toString());
        this.sentences = new ArrayList<Sentence<Token>>();
        this.systemCoord = null;
        this.timestamp = null;
    }

    public void addSentence(Sentence<Token> sentence) {
        sentences.add(sentence);
    }

    public Iterator<Sentence<Token>> iterator() {
        return sentences.iterator();
    }
}
