/*
 * This class takes the XML the Region Topic Model word-by-region probabilities output and generates a KML file for viewing in Google Earth.
 */

package opennlp.textgrounder.eval;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;

import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topostructs.*;

public class RegionTopicXMLtoKML {

    private static double RADIUS = .2;
    private static int SIDES = 10;
    private static int BARSCALE = 1000000;

    private static DocumentBuilderFactory dbf;
    private static DocumentBuilder db;

    public static void main(String[] args) throws Exception {
        RegionTopicXMLtoKML regionTopicXMLtoKML = new RegionTopicXMLtoKML(args[0], args[1]);
    }

    public RegionTopicXMLtoKML(String xmlInputPath, String kmlOutputPath) throws Exception {
        dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        db = dbf.newDocumentBuilder();

        Document doc = db.parse(xmlInputPath);

        BufferedWriter kmlOut = new BufferedWriter(new FileWriter(kmlOutputPath));
        kmlOut.write(KMLUtil.genKMLHeader(xmlInputPath));

        XPathFactory factory = XPathFactory.newInstance();
        XPath xpath = factory.newXPath();

        XPathExpression regionExpr = xpath.compile("//region");

        Object regionResult = regionExpr.evaluate(doc, XPathConstants.NODESET);
        NodeList regionNodes = (NodeList) regionResult;
        for (int regionNodeIndex = 0; regionNodeIndex < regionNodes.getLength(); regionNodeIndex++) {
            Node regionNode = regionNodes.item(regionNodeIndex);
            //System.out.println(regionNode.getNodeName());

            XPathExpression regionIdExpr = xpath.compile("@id");
            Object regionIdResult = regionIdExpr.evaluate(regionNode, XPathConstants.STRING);
            int regionId = Integer.parseInt((String) regionIdResult);
            //System.out.println(regionId);

            XPathExpression regionLatExpr = xpath.compile("@lat");
            Object regionLatResult = regionLatExpr.evaluate(regionNode, XPathConstants.NUMBER);
            double regionLat = (Double) regionLatResult;
            //System.out.println(regionLat);

            XPathExpression regionLonExpr = xpath.compile("@lon");
            Object regionLonResult = regionLonExpr.evaluate(regionNode, XPathConstants.NUMBER);
            double regionLon = (Double) regionLonResult;
            //System.out.println(regionLon);

            XPathExpression wordExpr = xpath.compile("word");

            Object wordResult = wordExpr.evaluate(regionNode, XPathConstants.NODESET);
            NodeList wordNodes = (NodeList) wordResult;
            for(int wordNodeIndex = 0; wordNodeIndex < wordNodes.getLength(); wordNodeIndex++) {
                Node wordNode = wordNodes.item(wordNodeIndex);
                //System.out.println(wordNode.getNodeName());

                XPathExpression wordTermExpr = xpath.compile("@term");
                Object wordTermResult = wordTermExpr.evaluate(wordNode, XPathConstants.STRING);
                String wordTerm = (String) wordTermResult;
                //System.out.println(wordTerm);

                XPathExpression wordProbExpr = xpath.compile("@prob");
                Object wordProbResult = wordProbExpr.evaluate(wordNode, XPathConstants.STRING);
                Double wordProb = Double.parseDouble((String)wordProbResult);
                //System.out.println(wordProb);

                Coordinate center = new Coordinate(regionLon, regionLat);
                Coordinate spiralPoint = center.getNthSpiralPoint(wordNodeIndex, .5);

                double height = wordProb * BARSCALE;

                String kmlPolygon = spiralPoint.toKMLPolygon(SIDES, RADIUS, height);

                kmlOut.write(KMLUtil.genPolygon("", spiralPoint, RADIUS, kmlPolygon));
                kmlOut.write(KMLUtil.genFloatingPlacemark(wordTerm, spiralPoint, height));
            }
        }

        kmlOut.write(KMLUtil.genKMLFooter());
        kmlOut.close();
    }
}
