/*
 * This class takes the XML the Region Topic Model word-by-region probabilities output and generates a KML file for viewing in Google Earth.
 */

package opennlp.textgrounder.eval;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import javax.xml.xpath.*;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.*;

import org.w3c.dom.*;

import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topostructs.*;

public class RegionTopicXMLtoKML {

    private static double RADIUS = .2;
    private static int SIDES = 10;
    private static int BARSCALE = 1000000;

    private static DocumentBuilderFactory dbf;
    private static DocumentBuilder db;

    public static void main(String[] args)
      throws FileNotFoundException, IOException, XMLStreamException {
      RegionTopicXMLtoKML.convert(args[0], args[1]);
    }

    public static void convert(String xmlInputPath, String kmlOutputPath)
      throws FileNotFoundException, IOException, XMLStreamException {
      XMLInputFactory inFactory = XMLInputFactory.newInstance();
      XMLOutputFactory outFactory = XMLOutputFactory.newInstance();

      XMLStreamReader in = inFactory.createXMLStreamReader(new BufferedReader(new FileReader(xmlInputPath)));
      XMLStreamWriter out = outFactory.createXMLStreamWriter(new BufferedWriter(new FileWriter(kmlOutputPath)));

      RegionTopicXMLtoKML.convert(in, out, xmlInputPath);
    }

    public static void convert(XMLStreamReader in, XMLStreamWriter out, String name)
      throws XMLStreamException {
      KMLUtil.writeHeader(out, name);

      in.nextTag();
      assert in.isStartElement() && in.getLocalName().equals("probabilities");
      in.nextTag();
      assert in.isStartElement() && in.getLocalName().equals("word-by-region");
      in.nextTag();

      while (in.isStartElement() && in.getLocalName().equals("region")) {
        double regionLat = Double.parseDouble(in.getAttributeValue(null, "lat"));
        double regionLon = Double.parseDouble(in.getAttributeValue(null, "lon"));
        Coordinate center = new Coordinate(regionLat, regionLon);

        in.nextTag();

        int i = 0;
        while (in.isStartElement() && in.getLocalName().equals("word")) {
          String term = in.getAttributeValue(null, "term");
          double prob = Double.parseDouble(in.getAttributeValue(null, "prob"));
          double height = prob * BARSCALE;

          Coordinate spiralPoint = center.getNthSpiralPoint(i, .5);

          KMLUtil.writePolygon(out, "", spiralPoint, SIDES, RADIUS, height);
          KMLUtil.writeFloatingPlacemark(out, term, spiralPoint, height);

          i++;
          in.nextTag();
          assert in.isEndElement() && in.getLocalName().equals("word");
          in.nextTag();
        }

        assert in.isEndElement() && in.getLocalName().equals("region");
        in.nextTag();
      }

      in.close();

      KMLUtil.writeFooter(out);
      out.close();
    }

    @Deprecated
    public RegionTopicXMLtoKML(String xmlInputPath, String kmlOutputPath) throws Exception {

        Document doc = db.parse(xmlInputPath);

        BufferedWriter kmlOut = new BufferedWriter(new FileWriter(kmlOutputPath));
        kmlOut.write(KMLUtil.genKMLHeader(xmlInputPath));

        XPathFactory factory = XPathFactory.newInstance();
        XPath xpath = factory.newXPath();

        XPathExpression regionExpr = xpath.compile("//word-by-region/region");
	XPathExpression regionIdExpr = xpath.compile("@id");
	XPathExpression regionLatExpr = xpath.compile("@lat");
	XPathExpression regionLonExpr = xpath.compile("@lon");
	
	XPathExpression wordExpr = xpath.compile("word");
	XPathExpression wordTermExpr = xpath.compile("@term");
	XPathExpression wordProbExpr = xpath.compile("@prob");

        Object regionResult = regionExpr.evaluate(doc, XPathConstants.NODESET);
        NodeList regionNodes = (NodeList) regionResult;
        for (int regionNodeIndex = 0; regionNodeIndex < regionNodes.getLength(); regionNodeIndex++) {
            Node regionNode = regionNodes.item(regionNodeIndex);
            //System.out.println(regionNode.getNodeName());

            Object regionIdResult = regionIdExpr.evaluate(regionNode, XPathConstants.STRING);
            int regionId = Integer.parseInt((String) regionIdResult);
            //System.out.println(regionId);

            Object regionLatResult = regionLatExpr.evaluate(regionNode, XPathConstants.NUMBER);
            double regionLat = (Double) regionLatResult;
            //System.out.println(regionLat);

            Object regionLonResult = regionLonExpr.evaluate(regionNode, XPathConstants.NUMBER);
            double regionLon = (Double) regionLonResult;
            //System.out.println(regionLon);

            Object wordResult = wordExpr.evaluate(regionNode, XPathConstants.NODESET);
            NodeList wordNodes = (NodeList) wordResult;
            for(int wordNodeIndex = 0; wordNodeIndex < wordNodes.getLength(); wordNodeIndex++) {
                Node wordNode = wordNodes.item(wordNodeIndex);
                //System.out.println(wordNode.getNodeName());

                Object wordTermResult = wordTermExpr.evaluate(wordNode, XPathConstants.STRING);
                String wordTerm = (String) wordTermResult;
                //System.out.println(wordTerm);

                Object wordProbResult = wordProbExpr.evaluate(wordNode, XPathConstants.STRING);
                Double wordProb = Double.parseDouble((String)wordProbResult);
                //System.out.println(wordProb);

                Coordinate center = new Coordinate(regionLat, regionLon);/////
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
