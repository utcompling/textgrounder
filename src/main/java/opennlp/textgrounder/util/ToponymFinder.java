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
	private BufferedReader gazetteer;
	private HashSet<String> toponymsInGaz;
	
	public ToponymFinder(BufferedReader reader, String gazInputPath) throws InvalidFormatException, IOException{
		sentDivider = new OpenNLPSentenceDivider();
		tokenizer = new OpenNLPTokenizer();
		recognizer = new OpenNLPRecognizer();
		this.input = reader;
		toponymsInGaz = new HashSet<String>();
		if(gazInputPath.toLowerCase().endsWith(".zip")) {
            ZipFile zf = new ZipFile(gazInputPath);
            ZipInputStream zis = new ZipInputStream(new FileInputStream(gazInputPath));
            ZipEntry ze = zis.getNextEntry();
            gazetteer = new BufferedReader(new InputStreamReader(zf.getInputStream(ze)));
            zis.close();
        }
        else {
        	gazetteer = new BufferedReader(new FileReader(gazInputPath));
        }
		String line;
		while((line=gazetteer.readLine())!=null){
			toponymsInGaz.add(line.split("\t")[1]);
		}
	}


	public static void main(String[] args) throws IOException {
		ToponymFinder finder = new ToponymFinder(new BufferedReader(new FileReader("TheStory.txt")),"./data/gazetteers/US.txt");
		finder.find();
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
					String resultToken="";
					for(int i=span.getStart();i<span.getEnd();i++){
						resultToken = resultToken + " " +tokens.get(i);
					}
					
					if(span.getItem()==NamedEntityType.LOCATION){
						resultSet.add(resultToken);
					}else if(toponymsInGaz.contains(resultToken)){
						resultSet.add(resultToken);
					}
				}
			}
		}
		return resultSet;
	}

}
