/*
 * This class converts the output of the region topic model (typically "output.xml") to the format required by EvalBasedOnXML.
 * The first argument is the path to that output.xml and the second argument is the desired path to the converted file.
 */

package opennlp.textgrounder.eval;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;

import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.*;

public class RegionTopicXMLtoEvaluableXML {

    private static final int CONTEXT_WINDOW_SIZE = 20;
    private static final int CONTEXT_SIGNATURE_WINDOW_SIZE = 20;

    private static DocumentBuilderFactory dbf;
    private static DocumentBuilder db;
    private static XPathFactory xpf;
    private static XPath xpath;

    public static void main(String[] args) throws Exception {
        RegionTopicXMLtoEvaluableXML regionTopicXMLtoEvaluableXML = new RegionTopicXMLtoEvaluableXML(args[0], args[1]);
    }

    public RegionTopicXMLtoEvaluableXML(String inputXMLPath, String outputXMLPath) throws Exception {
        
        dbf = DocumentBuilderFactory.newInstance();
        db = dbf.newDocumentBuilder();
        xpf = XPathFactory.newInstance();
        xpath = xpf.newXPath();

        Document outputDoc = db.newDocument();
        Element toponymsE = outputDoc.createElement("toponyms");

        Document inputDoc = db.parse(inputXMLPath);

        String[] allTokens = XMLUtil.getAllTokens(inputDoc, true);
        int tokenIndex = -1;

        XPathExpression docExpr = xpath.compile("//doc");
        //XPathExpression idExpr = xpath.compile("@id");
        XPathExpression sExpr = xpath.compile("s");
        XPathExpression latExpr = xpath.compile("@lat");
        XPathExpression longExpr = xpath.compile("@long");

        Object docResult = docExpr.evaluate(inputDoc, XPathConstants.NODESET);
        NodeList docNodes = (NodeList) docResult;

        for(int docIndex = 0; docIndex < docNodes.getLength(); docIndex++) {
            Node curDocNode = docNodes.item(docIndex);

            //Object idResult = idExpr.evaluate(curDocNode, XPathConstants.NUMBER);
            //int curDocId = (Integer) idResult;

            Object sResult = sExpr.evaluate(curDocNode, XPathConstants.NODESET);
            NodeList sNodes = (NodeList) sResult;

            //System.out.println(sNodes.getLength());
            for(int sIndex = 0; sIndex < sNodes.getLength(); sIndex++) {
                Node curSNode = sNodes.item(sIndex);

                //idResult = idExpr.evaluate(curSNode, XPathConstants.NUMBER);
                //int curSId = (Integer) idResult;

                NodeList tokens = curSNode.getChildNodes();

                for(int tIndex = 0; tIndex < tokens.getLength(); tIndex++) {
                    Node tokenNode = tokens.item(tIndex);
                    if(tokenNode.getNodeName().equals("toponym")) {
                        tokenIndex++;

                        // checks if this toponym is unresolved (selected="NoneFromList") or not really a location (selected="NotALocation")
                        if(tokenNode.getAttributes().getNamedItem("selected") != null)
                            continue;

                        Node toponymNode = outputDoc.importNode(tokenNode, false);
                        Element locationE = outputDoc.createElement("location");
                        Element contextE = outputDoc.createElement("context");

                        String latResult = (String) latExpr.evaluate(toponymNode, XPathConstants.STRING);
                        //double lat = Double.parseDouble((String) latResult);

                        String lonResult = (String) longExpr.evaluate(toponymNode, XPathConstants.STRING);
                        //double lon = Double.parseDouble((String) lonResult);

                        if(latResult == null || lonResult == null || latResult.length() == 0 || lonResult.length() == 0)
                            continue;

                        //System.out.println((String)latResult);

                        toponymNode.getAttributes().removeNamedItem("lat"); // deal with this better
                        toponymNode.getAttributes().removeNamedItem("long");

                        locationE.setAttribute("lat", latResult);
                        locationE.setAttribute("long", lonResult);
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

        outputDoc.appendChild(toponymsE);
        XMLUtil.writeDocToFile(outputDoc, outputXMLPath);
    }
}
