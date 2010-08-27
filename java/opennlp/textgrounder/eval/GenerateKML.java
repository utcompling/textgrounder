/*
 * This class generates the KML output for Google Earth given a model's output in XML format
 */

package opennlp.textgrounder.eval;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topostructs.*;

public class GenerateKML {

    private static DocumentBuilderFactory dbf;
    private static DocumentBuilder db;

    public static void main(String[] args) throws Exception {
        GenerateKML generateKML = new GenerateKML(args[0], args[1]);
    }

    public GenerateKML(String modelOutputXMLPath, String kmlOutputPath) throws Exception {

        dbf = DocumentBuilderFactory.newInstance();
        db = dbf.newDocumentBuilder();

        outputKML(modelOutputXMLPath, kmlOutputPath);
    }

    public void outputKML(String modelOutputXMLPath, String kmlOutputPath) throws Exception {

        BufferedWriter kmlOut = new BufferedWriter(new FileWriter(kmlOutputPath));
        int dotKmlIndex = kmlOutputPath.lastIndexOf(".kml");
        String contextFilename = kmlOutputPath.substring(0, dotKmlIndex) + "-context.kml";
        BufferedWriter contextOut = new BufferedWriter(new FileWriter(contextFilename));

        kmlOut.write(KMLUtil.genKMLHeader(modelOutputXMLPath));
        contextOut.write(KMLUtil.genKMLHeader(modelOutputXMLPath));

        Document modeldoc = db.parse(modelOutputXMLPath);

        NodeList modelToponyms = modeldoc.getChildNodes().item(0).getChildNodes();

        HashMap<String, ArrayList<String>> locationsToContexts = new HashMap<String, ArrayList<String>>();
        HashMap<String, Location> locationsToLocationData = new HashMap<String, Location>();

        for(int i = 0; i < modelToponyms.getLength(); i++) {
            Node ModelTopN = modelToponyms.item(i);

            if(!ModelTopN.getNodeName().equals("toponym"))
                continue;

            Node modelLocationN = ModelTopN.getChildNodes().item(1);

            String modelLat = /*Double.parseDouble(*/modelLocationN.getAttributes().getNamedItem("lat").getNodeValue()/*)*/;
            String modelLon = /*Double.parseDouble(*/modelLocationN.getAttributes().getNamedItem("long").getNodeValue()/*)*/;
            String modelContext = ModelTopN.getChildNodes().item(3).getTextContent();

            ArrayList<String> curContextsList = locationsToContexts.get(modelLat + ";" + modelLon);
            if(curContextsList == null)
                curContextsList = new ArrayList<String>();
            curContextsList.add(modelContext);

            locationsToContexts.put(modelLat + ";" + modelLon, curContextsList);

            Location curLocationData = locationsToLocationData.get(modelLat + ";" + modelLon);
            if(curLocationData == null) {
                curLocationData = new Location();
                curLocationData.setName(ModelTopN.getAttributes().getNamedItem("term").getNodeValue().toLowerCase());
                curLocationData.setCoord(new Coordinate(Double.parseDouble(modelLon), Double.parseDouble(modelLat)));//////////////
                locationsToLocationData.put(modelLat + ";" + modelLon, curLocationData);
            }
            
        }

        String[] keys = locationsToContexts.keySet().toArray(new String[0]);
        for(int i = 0; i < keys.length; i++) {
            String curKey = keys[i];
            
            int semicolonIndex = curKey.indexOf(';');

            double curLat = Double.parseDouble(curKey.substring(0, semicolonIndex));
            double curLon = Double.parseDouble(curKey.substring(semicolonIndex+1));

            ArrayList<String> contextsAtThisLocation = locationsToContexts.get(curKey);
            Location curLocationData = locationsToLocationData.get(curLat + ";" + curLon);

            double height = Math.log(contextsAtThisLocation.size()) * 50000;
            String kmlPolygon = curLocationData.getCoord().toKMLPolygon(10, .15, height);
            kmlOut.write(KMLUtil.genPolygon(curLocationData.getName(), curLocationData.getCoord(), .15, kmlPolygon));

            for(int j = 0; j < contextsAtThisLocation.size(); j++) {
                String curContext = contextsAtThisLocation.get(j);
                contextOut.write(KMLUtil.genSpiralpoint(curLocationData.getName(),
                        curContext.replaceAll("\\&", "AND").replaceAll("\\[h\\]", "<b> ").replaceAll("\\[\\/h\\]", "</b>"),
                        curLocationData.getCoord().getNthSpiralPoint(j, 0.13), j, .15));
            }
        }

        kmlOut.write(KMLUtil.genKMLFooter());
        contextOut.write(KMLUtil.genKMLFooter());

        kmlOut.close();
        contextOut.close();
    }
}
