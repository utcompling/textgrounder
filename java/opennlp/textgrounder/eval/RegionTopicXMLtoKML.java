/*
 * This class takes the XML the Region Topic Model word-by-region probabilities output and generates a KML file for viewing in Google Earth.
 */

package opennlp.textgrounder.eval;

import java.io.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.*;

import opennlp.textgrounder.util.KMLUtil;
import opennlp.textgrounder.topostructs.*;

public class RegionTopicXMLtoKML {

    private static double RADIUS = .2;
    private static int SIDES = 10;
    private static int BARSCALE = 1000000;

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

        for (int i = 0; in.isStartElement() && in.getLocalName().equals("word"); i++) {
          String term = in.getAttributeValue(null, "term");
          double prob = Double.parseDouble(in.getAttributeValue(null, "prob"));
          double height = prob * BARSCALE;

          Coordinate spiralPoint = center.getNthSpiralPoint(i, .5);

          KMLUtil.writePolygon(out, "", spiralPoint, SIDES, RADIUS, height);
          KMLUtil.writeFloatingPlacemark(out, term, spiralPoint, height);

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
}

