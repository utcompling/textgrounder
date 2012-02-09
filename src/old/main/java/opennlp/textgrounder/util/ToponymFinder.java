/**
 * 
 */
package opennlp.textgrounder.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import opennlp.textgrounder.text.prep.HighRecallToponymRecognizer;
import opennlp.textgrounder.text.prep.NamedEntityRecognizer;
import opennlp.textgrounder.text.prep.NamedEntityType;
import opennlp.textgrounder.text.prep.OpenNLPRecognizer;
import opennlp.textgrounder.text.prep.OpenNLPSentenceDivider;
import opennlp.textgrounder.text.prep.OpenNLPTokenizer;
import opennlp.textgrounder.text.prep.SentenceDivider;
import opennlp.textgrounder.text.prep.Tokenizer;
import opennlp.textgrounder.topo.gaz.GeoNamesGazetteer;
import opennlp.textgrounder.util.Span;
import opennlp.tools.util.InvalidFormatException;

/**
 * @author abhimanu kumar
 *
 */
public class ToponymFinder {

	/**
	 * @param args
	 */
	private final SentenceDivider sentDivider;
	private final Tokenizer tokenizer;
	private final NamedEntityRecognizer recognizer;
	private BufferedReader input;
	
	public ToponymFinder(BufferedReader reader, String gazPath) throws Exception{
		sentDivider = new OpenNLPSentenceDivider();
		tokenizer = new OpenNLPTokenizer();
		recognizer = new HighRecallToponymRecognizer(gazPath);
		this.input = reader;
	}


	public static void main(String[] args) throws Exception {
		ToponymFinder finder = new ToponymFinder(new BufferedReader(new FileReader(args[0]/*"TheStoryTemp.txt"*/)),args[1]/*"data/gazetteers/US.ser.gz"*/);
//		long startTime = System.currentTimeMillis();
		finder.find();
//		long stopTime = System.currentTimeMillis();
//		System.out.println((stopTime-startTime)/1000 + "secs");
	}


	private HashSet<String> find() throws IOException {
		String line; 
		HashSet<String> resultSet = new HashSet<String>();
		while((line=input.readLine())!=null){
			List<String> sentencesString = sentDivider.divide(line);
			for (String sentence : sentencesString){
				List<String> tokens = new ArrayList<String>();
				for(String token : tokenizer.tokenize(sentence)){
					tokens.add(token);
				}
				List<Span<NamedEntityType>> spans =recognizer.recognize(tokens);
				for(Span<NamedEntityType> span:spans){
					StringBuilder resultToken= new StringBuilder();
					for(int i=span.getStart();i<span.getEnd();i++){
						resultToken = resultToken.append(" ").append(tokens.get(i));
					}
					resultSet.add(resultToken.toString());
				}
				
			}
		}
//		System.out.println(resultSet);
		return resultSet;
	}

}
