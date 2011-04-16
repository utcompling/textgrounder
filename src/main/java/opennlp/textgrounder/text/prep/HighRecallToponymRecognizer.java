/**
 * 
 */
package opennlp.textgrounder.text.prep;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import opennlp.textgrounder.topo.gaz.GeoNamesGazetteer;
import opennlp.textgrounder.util.Constants;
import opennlp.textgrounder.util.Span;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.HashList;
import opennlp.tools.util.InvalidFormatException;
import scala.actors.threadpool.Arrays;

/**
 * @author abhimanu kumar
 *
 */
public class HighRecallToponymRecognizer extends OpenNLPRecognizer {
	private HashMap<String, Integer[]> nameHashMap;
	private HashMap<String, Integer> stringInternHashMap;
	private TokenNameFinder personFinder;
	private TokenNameFinder orgFinder;
	private Pattern splitPattern;
	private Pattern historicalPattern;
	private static final String spaceRegEx = " ";
	private static final String historyRegEx = "(historical)";
	private boolean flagStart = false;
	private int stringCount=Integer.MIN_VALUE;
	private int lineCount=0;

	public HighRecallToponymRecognizer(GeoNamesGazetteer gnGz) throws IOException, InvalidFormatException {
		super();
		getCleanedNameSet(gnGz.getUniqueLocationNameSet());
		getNLPModels();
		
	}



	private void getNLPModels() throws InvalidFormatException, IOException {
		personFinder=new NameFinderME(new TokenNameFinderModel(new FileInputStream(
				Constants.getOpenNLPModelsDir() + File.separator + "en-ner-person.bin")));
		orgFinder=new NameFinderME(new TokenNameFinderModel(new FileInputStream(
				Constants.getOpenNLPModelsDir() + File.separator + "en-ner-organization.bin")));
	}



	private void getCleanedNameSet(Set<String> keySet) {
//		System.out.println("Formatting Locations...");
		this.nameHashMap = new HashMap<String,Integer[]>(500000);
		this.stringInternHashMap = new HashMap<String, Integer>(100000);
		compilePatterns();
		for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {
			String toponym = (String) iterator.next();
			toponym = historicalPattern.matcher(toponym).replaceAll("");
			
			
			if(!nameHashMap.containsKey(toponym)){
				String[] toponymTokens = splitPattern.split(toponym);
				int xorNum = 0;
				ArrayList<Integer> tokenIntList = new ArrayList<Integer>();
				for (int i = 0; i < toponymTokens.length; i++) {
					String token = toponymTokens[i];//.trim();
					if(stringInternHashMap.containsKey(token)){
						tokenIntList.add(stringInternHashMap.get(token));
					}else{
						stringInternHashMap.put(token, stringCount);
						tokenIntList.add(stringCount);
						stringCount++;
					}
				}
				nameHashMap.put(toponym,tokenIntList.toArray(new Integer[0]));
			}
		}
//		System.out.println(nameHashMap.size());
//		System.out.println(stringInternHashMap.size());
	}



	private void compilePatterns() {
		splitPattern = Pattern.compile(spaceRegEx); 
		historicalPattern = Pattern.compile(historyRegEx);		
	}



	public HighRecallToponymRecognizer(String gazPath) throws Exception{
		super();
		GZIPInputStream gis;
		ObjectInputStream ois;
		GeoNamesGazetteer gnGaz = null;
		gis = new GZIPInputStream(new FileInputStream(gazPath));
		ois = new ObjectInputStream(gis);
		gnGaz = (GeoNamesGazetteer) ois.readObject();
		getCleanedNameSet(gnGaz.getUniqueLocationNameSet());
		getNLPModels();
	}

	public HighRecallToponymRecognizer(Set<String> uniqueLocationNameSet) throws IOException, InvalidFormatException {
		super();
		getCleanedNameSet(uniqueLocationNameSet);
		getNLPModels();
	}



	public List<Span<NamedEntityType>> recognize(List<String> tokens) {
		if(!flagStart){
			System.out.print("\nRaw Corpus: Searching for Toponyms ");
			flagStart=true;
		}
		if(lineCount==1000){
			System.out.print(".");
			lineCount=0;
		}
		lineCount++;
		List<Span<NamedEntityType>> spans = new ArrayList<Span<NamedEntityType>>();
		String[] tokensToBeLookedArray = (String[]) Arrays.copyOf(tokens.toArray(),tokens.toArray().length,String[].class);
		for (opennlp.tools.util.Span span : this.finder.find(tokens.toArray(new String[0]))) {
			spans.add(new Span<NamedEntityType>(span.getStart(), span.getEnd(), this.type));

			for (int i = span.getStart(); i < span.getEnd(); i++) {
				tokensToBeLookedArray[i]=" ";
			}
		}

		for (opennlp.tools.util.Span span : this.personFinder.find(tokens.toArray(new String[0]))) {
			for (int i = span.getStart(); i < span.getEnd(); i++) {
//				System.out.println("PERSON "+tokensToBeLookedArray[i]);
				tokensToBeLookedArray[i]=" ";
			}
		}

		for (opennlp.tools.util.Span span : this.orgFinder.find(tokens.toArray(new String[0]))) {
			for (int i = span.getStart(); i < span.getEnd(); i++) {
//				System.out.println("ORG "+tokensToBeLookedArray[i]);
				tokensToBeLookedArray[i]=" ";
			}
		}

		for (int i = 0; i < tokensToBeLookedArray.length; i++) {
			String token = tokensToBeLookedArray[i];
			if(token.length()==1)
				continue;;
				if(startsWithCaps(token)){
					boolean matched=true; 
					int k=0;
					for (Iterator iterator = nameHashMap.keySet().iterator(); iterator.hasNext();) {
						String toponymTokens = ((String) iterator.next());
							
						int toponymLength=nameHashMap.get(toponymTokens).length;
						if(toponymLength>tokensToBeLookedArray.length-i)
							continue;
						
						matched=true;
							
						ArrayList<Integer> suspectedTokenSet = new ArrayList<Integer>();
						for (int j = 0; j < toponymLength; j++) {
							if(!startsWithCaps(tokensToBeLookedArray[j+i])){
								matched=false;
								break;
							}
							suspectedTokenSet.add(stringInternHashMap.get(tokensToBeLookedArray[j+i].toLowerCase()));
							
						}
						if(!stringMatch(suspectedTokenSet.toArray(new Integer[0]),nameHashMap.get(toponymTokens))){
							matched=false;
							continue;
						}
						if(matched){
							k=i+toponymLength-1;
							break;
						}
					}
					if(matched){
//						String startToken = tokensToBeLookedArray[i];
//						String endToken = tokensToBeLookedArray[k];
						spans.add(new Span<NamedEntityType>(i, k+1, this.type));
						i=k;
//						System.out.println(startToken+endToken);
					}
				}
		}
		return spans;
	}


	private boolean stringMatch(Integer[] suspectedSet,Integer[] toponymSet) {
		for (int i = 0; i < suspectedSet.length; i++) {
			if(suspectedSet[i]!=toponymSet[i])
				return false;
		}
		return true;
	}
	
	private boolean stringMatch(StringBuilder tokenCombined,
			StringBuilder toponymCombined) {
		if(tokenCombined.length()!=toponymCombined.length())
			return false;
		for (int i = 0; i < tokenCombined.length(); i++) {
			if(tokenCombined.charAt(i)!=toponymCombined.charAt(i))
				return false;
		}
		return true;
	}



	private boolean startsWithCaps(String tobeLooked) {
		return new Integer('A')<=new Integer(tobeLooked.charAt(0)) && new Integer(tobeLooked.charAt(0))<=new Integer('Z');
	}
	
	
}

class HashString {
	
	private String string;
	private int hash;

	HashString(String string ,int hash){
		this.string=string;
		this.hash=hash;
	}
	
	public String toString(){
		return string;
	}
	
	public int hashCode() {
	        return hash;
	    }
}
