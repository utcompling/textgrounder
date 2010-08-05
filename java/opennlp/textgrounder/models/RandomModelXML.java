/*
 * This is the version of the Random baseline model that reads in XML format that matches the TR-CoNLL XML format
 * and writes in the format required as input to EvalBasedOnXML.java.
 * The input will not have any cand elements with selected="yes" (or they will be ignored if present), and the output
 * will the following form:

 <toponyms>
  <toponym term='Israel' did='d100' sid='1' tid='71'>
    <location lat='31' long='35'/>
    <context>They arrived in [h]Israel[/h] , where officials</context>
  </toponym>
  [...more toponyms...]
</toponyms>

 */

package opennlp.textgrounder.models;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

import opennlp.textgrounder.util.*;

public class RandomModelXML extends ModelXML {

    private static Random myRandom = new Random();

    public static void main(String[] args) throws Exception {
        RandomModelXML randomModelXML = new RandomModelXML(args[0], args[1]);
        //ModelXML.main(args);
    }
    
    public RandomModelXML(String inputXMLPath, String outputXMLPath) throws Exception {
        super(inputXMLPath, outputXMLPath);
        /*dbf = DocumentBuilderFactory.newInstance();
        db = dbf.newDocumentBuilder();

        disambiguateToponyms(inputXMLPath, outputXMLPath);*/
    }

    @Override
    protected void disambiguateToponyms(String inputXMLPath, String outputXMLPath) throws Exception {
        Document outputDoc = db.newDocument();
        File inputXMLFile = new File(inputXMLPath);

        Element toponymsE = outputDoc.createElement("toponyms");

        if(inputXMLFile.isDirectory()) {

            for(String filename : inputXMLFile.list()) {
                if(filename.toLowerCase().endsWith(".xml")) {
                    disambiguateToponymsInSingleFile(inputXMLFile.getCanonicalPath() + File.separator + filename, outputDoc, toponymsE);
                }
            }
        }
        else {
            disambiguateToponymsInSingleFile(inputXMLPath, outputDoc, toponymsE);
        }

        outputDoc.appendChild(toponymsE);

        XMLUtil.writeDocToFile(outputDoc, outputXMLPath);
    }

    private void disambiguateToponymsInSingleFile(String inputXMLPath, Document outputDoc, Element toponymsE) throws Exception {
        Document inputDoc = db.parse(inputXMLPath);

        String[] allTokens = XMLUtil.getAllTokens(inputDoc, true);
        int tokenIndex = -1;

        //NodeList sentences = inputDoc.getChildNodes().item(1).getChildNodes();
        NodeList corpusDocs = inputDoc.getChildNodes().item(0).getChildNodes();

        for(int d = 0; d < corpusDocs.getLength(); d++) {
            if(!corpusDocs.item(d).getNodeName().equals("doc"))
                continue;

            NodeList sentences = corpusDocs.item(d).getChildNodes();

            for(int i = 0; i < sentences.getLength(); i++) {
                if(!sentences.item(i).getNodeName().equals("s"))
                    continue;
                NodeList tokens = sentences.item(i).getChildNodes();
                for(int j = 0; j < tokens.getLength(); j++) {
                    Node tokenNode = tokens.item(j);
                    if(tokenNode.getNodeName().equals("toponym")) {
                        tokenIndex++;

                        // checks if this toponym is unresolved (selected="NoneFromList") or not really a location (selected="NotALocation")
                        if(tokenNode.getAttributes().getNamedItem("selected") != null)
                            continue;

                        Node candidatesNode = tokenNode.getChildNodes().item(1);
                        NodeList candidates = candidatesNode.getChildNodes();
                        if(candidates.getLength() < 3)
                            continue;

                        int selectedIndex = disambiguateFromCandidateSet(candidates);

                        Node toponymNode = outputDoc.importNode(tokenNode, false);
                        Element locationE = outputDoc.createElement("location");
                        Element contextE = outputDoc.createElement("context");

                        Node candidateNode = candidates.item(selectedIndex);
                        assert(candidateNode.getNodeName().equals("cand"));

                        locationE.setAttribute("lat", candidateNode.getAttributes().getNamedItem("lat").getNodeValue());
                        locationE.setAttribute("long", candidateNode.getAttributes().getNamedItem("long").getNodeValue());
                        toponymNode.appendChild(locationE);

                        contextE.setTextContent(StringUtil.join(XMLUtil.getContextWindow(allTokens, tokenIndex, CONTEXT_WINDOW_SIZE)));
                        toponymNode.appendChild(contextE);

                        toponymsE.appendChild(toponymNode);
                    }
                    else if(tokenNode.getNodeName().equals("w")) {
                        tokenIndex++;
                    }
                }
            }
        }
    }

    private int disambiguateFromCandidateSet(NodeList candidates) {
        // These math acrobatics are necessary due to the #text elements between each cand element:
        int randIndex = myRandom.nextInt((candidates.getLength()-1)/2);
        randIndex = (randIndex * 2) + 1;

        return randIndex;
    }
}
