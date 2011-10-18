package opennlp.textgrounder.text;

import java.io.*;
import java.util.*;

import opennlp.textgrounder.topo.*;

public class GeoTextDocument extends Document {

    private static final long serialVersionUID = 42L;

    private List<Sentence<Token>> sentences;

    public GeoTextDocument(String id, String timestamp, double goldLat, double goldLon) {
	super(id);
	this.timestamp = timestamp;
	this.goldCoord = Coordinate.fromDegrees(goldLat, goldLon);
        this.sentences = new ArrayList<Sentence<Token>>();
        this.systemCoord = null;
        this.timestamp = null;
    }

    public GeoTextDocument(String id, String timestamp, double goldLat, double goldLon, Enum<Document.SECTION> section) {
        this(id, timestamp, goldLat, goldLon);
        this.section = section;
    }

    public GeoTextDocument(String id, String timestamp, double goldLat, double goldLon, long fold) {
        this(id, timestamp, goldLat, goldLon);
        if(fold >= 1 && fold <= 3)
            this.section = Document.SECTION.TRAIN;
        else if(fold == 4)
            this.section = Document.SECTION.DEV;
        else if(fold == 5)
            this.section = Document.SECTION.TEST;
        else
            this.section = Document.SECTION.ANY;
    }

    public void addSentence(Sentence<Token> sentence) {
        sentences.add(sentence);
    }

    public Iterator<Sentence<Token>> iterator() {
        return sentences.iterator();
    }
}
