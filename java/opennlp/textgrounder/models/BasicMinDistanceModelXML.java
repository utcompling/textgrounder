/*
 * This is the version of the basic minimum distance (BMD) model that takes its input and output in XML.
 */

package opennlp.textgrounder.models;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import gnu.trove.*;

import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topostructs.*;

public class BasicMinDistanceModelXML extends ModelXML {

    public static void main(String[] args) throws Exception {
        BasicMinDistanceModelXML basicMinDistanceModelXML = new BasicMinDistanceModelXML(args[0], args[1]);
    }

    public BasicMinDistanceModelXML(String inputXMLPath, String outputXMLPath) throws Exception {
        super(inputXMLPath, outputXMLPath);
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

            for(int outerSentIndex = 0; outerSentIndex < sentences.getLength(); outerSentIndex++) {
                if(!sentences.item(outerSentIndex).getNodeName().equals("s"))
                    continue;
                NodeList tokens = sentences.item(outerSentIndex).getChildNodes();
                for(int outerTokenIndex = 0; outerTokenIndex < tokens.getLength(); outerTokenIndex++) {
                    Node tokenNode = tokens.item(outerTokenIndex);
                    if(tokenNode.getNodeName().equals("toponym")) {
                        tokenIndex++;

                        // checks if this toponym is unresolved (selected="NoneFromList") or not really a location (selected="NotALocation")
                        if(tokenNode.getAttributes().getNamedItem("selected") != null)
                            continue;

                        Node candidatesNode = tokenNode.getChildNodes().item(1);
                        NodeList candidates = candidatesNode.getChildNodes();
                        if(candidates.getLength() < 3)
                            continue;

                        int selectedIndex = disambiguateFromCandidateSet(candidates, corpusDocs.item(d));
                        

                        Node toponymNode = outputDoc.importNode(tokenNode, false);
                        Element locationE = outputDoc.createElement("location");
                        Element contextE = outputDoc.createElement("context");

                        Node candidateNode = candidates.item(selectedIndex);
                        assert(candidateNode.getNodeName().equals("cand") == true);

                        /*System.out.println(selectedIndex);
                        System.out.println(candidateNode.getNodeName());
                        System.out.println(candidateNode.getAttributes().getLength());
                        System.out.println(candidateNode.getAttributes().getNamedItem("lat"));*/

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

    private int disambiguateFromCandidateSet(NodeList candidates, Node curDocNode) {

        // singleton case: "3" actually means 1 here, due to the extra #text nodes in-between each cand node
        if(candidates.getLength() == 3)
            return 1;

        TObjectDoubleHashMap<String> totalDistances = new TObjectDoubleHashMap<String>();
        TObjectIntHashMap<String> idsToIndeces = new TObjectIntHashMap<String>();

        NodeList sentences = curDocNode.getChildNodes();

        for(int outerCandIndex = 0; outerCandIndex < candidates.getLength(); outerCandIndex++) {
            Node outerCandNode = candidates.item(outerCandIndex);
            if(!outerCandNode.getNodeName().equals("cand"))
                continue;

            String outerCandId = outerCandNode.getAttributes().getNamedItem("id").getNodeValue();
            idsToIndeces.put(outerCandId, outerCandIndex);
            Coordinate outerCandCoord = new Coordinate(Double.parseDouble(outerCandNode.getAttributes().getNamedItem("lat").getNodeValue()),
                                                    Double.parseDouble(outerCandNode.getAttributes().getNamedItem("long").getNodeValue()));

            for(int innerSentIndex = 0; innerSentIndex < sentences.getLength(); innerSentIndex++) {
                if(!sentences.item(innerSentIndex).getNodeName().equals("s"))
                    continue;
                NodeList tokens = sentences.item(innerSentIndex).getChildNodes();
                for(int innerTokenIndex = 0; innerTokenIndex < tokens.getLength(); innerTokenIndex++) {
                    Node tokenNode = tokens.item(innerTokenIndex);
                    if(tokenNode.getNodeName().equals("toponym")) {
                        //tokenIndex++;

                        // checks if this toponym is unresolved (selected="NoneFromList") or not really a location (selected="NotALocation")
                        if(tokenNode.getAttributes().getNamedItem("selected") != null)
                            continue;

                        Node candidatesNode = tokenNode.getChildNodes().item(1);
                        NodeList curCandidates = candidatesNode.getChildNodes();
                        if(curCandidates.getLength() < 3)
                            continue;

                        //System.out.println(curDocNode.getAttributes().getNamedItem("id").getNodeValue());
                        //System.out.println(tokenNode.getAttributes().getNamedItem("term").getNodeValue());

                        double minDistance = Double.MAX_VALUE;
                        for(int innerCandIndex = 0; innerCandIndex < curCandidates.getLength(); innerCandIndex++) {
                            Node innerCandNode = curCandidates.item(innerCandIndex);

                            if(!innerCandNode.getNodeName().equals("cand"))
                                continue;

                            //String innerCandId = innerCandNode.getAttributes().getNamedItem("id").getNodeValue();
                            Coordinate innerCandCoord = new Coordinate(Double.parseDouble(innerCandNode.getAttributes().getNamedItem("lat").getNodeValue()),
                                                    Double.parseDouble(innerCandNode.getAttributes().getNamedItem("long").getNodeValue()));

                            
                            double curDistance = outerCandCoord.computeDistanceTo(innerCandCoord);
                            if(curDistance < minDistance) {
                                minDistance = curDistance;
                            }                            
                        }
                        totalDistances.put(outerCandId, totalDistances.get(outerCandId) + minDistance);
                    }
                }
            }
        }

        String idToReturn = null;
        
        for(int outerCandIndex = 0; outerCandIndex < candidates.getLength(); outerCandIndex++) {
            Node outerCandNode = candidates.item(outerCandIndex);
            if(!outerCandNode.getNodeName().equals("cand"))
                continue;

            double minTotalDistance = Double.MAX_VALUE;
            for(String outerCandId : totalDistances.keys(new String[0])) {
                double curTotalDistance = totalDistances.get(outerCandId);
                if(curTotalDistance < minTotalDistance) {
                    curTotalDistance = minTotalDistance;
                    idToReturn = outerCandId;
                }
            }
        }

        return idsToIndeces.get(idToReturn);
    }
}
