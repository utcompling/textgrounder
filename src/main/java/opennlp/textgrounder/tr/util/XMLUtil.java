/*
 * This class contains utility functions for dealing with XML programmatically, such as writing a Document to an XML file.
 */

package opennlp.textgrounder.tr.util;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import org.w3c.dom.*;

public class XMLUtil {

    public static void writeDocToFile(Document doc, String filename) throws Exception {
        Source source = new DOMSource(doc);
        File file = new File(filename);
        Result result = new StreamResult(file);
        TransformerFactory tFactory = TransformerFactory.newInstance();
        tFactory.setAttribute("indent-number", 2);
        Transformer xformer = tFactory.newTransformer();
        xformer.setOutputProperty(OutputKeys.INDENT, "yes");
        //xformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        xformer.transform(source, result);

        /*OutputFormat format = new OutputFormat(doc);
        format.setIndenting(true);
        format.setIndent(2);
        Writer output = new BufferedWriter( new FileWriter(filename) );
        XMLSerializer serializer = new XMLSerializer(output, format);
        serializer.serialize(doc);*/
    }

    public static String[] getAllTokens(Document doc, boolean hasCorpusElement) {
        ArrayList<String> toReturnAL = new ArrayList<String>();

        if(hasCorpusElement) {

            NodeList corpusDocs = doc.getChildNodes().item(0).getChildNodes();

            for(int d = 0; d < corpusDocs.getLength(); d++) {
                if(!corpusDocs.item(d).getNodeName().equals("doc"))
                    continue;

                //System.out.println(doc.getChildNodes().getLength());

                NodeList sentences = corpusDocs.item(d).getChildNodes();

                for(int i = 0; i < sentences.getLength(); i++) {
                    if(!sentences.item(i).getNodeName().equals("s"))
                        continue;
                    NodeList tokens = sentences.item(i).getChildNodes();
                    for(int j = 0; j < tokens.getLength(); j++) {
                        Node tokenNode = tokens.item(j);
                        if(tokenNode.getNodeName().equals("toponym")) {
                            toReturnAL.add(tokenNode.getAttributes().getNamedItem("term").getNodeValue());
                        }
                        else if(tokenNode.getNodeName().equals("w")) {
                            toReturnAL.add(tokenNode.getAttributes().getNamedItem("tok").getNodeValue());
                        }

                    }
                }
            }
        }
        else {
            NodeList sentences = doc.getChildNodes().item(1).getChildNodes();

            for(int i = 0; i < sentences.getLength(); i++) {
                if(!sentences.item(i).getNodeName().equals("s"))
                    continue;
                NodeList tokens = sentences.item(i).getChildNodes();
                for(int j = 0; j < tokens.getLength(); j++) {
                    Node tokenNode = tokens.item(j);
                    if(tokenNode.getNodeName().equals("toponym")) {
                        toReturnAL.add(tokenNode.getAttributes().getNamedItem("term").getNodeValue());
                    }
                    else if(tokenNode.getNodeName().equals("w")) {
                        toReturnAL.add(tokenNode.getAttributes().getNamedItem("tok").getNodeValue());
                    }

                }
            }
        }

        return toReturnAL.toArray(new String[0]);
    }

    public static String[] getAllTokens(Document doc) {
        return getAllTokens(doc, false);
        /*ArrayList<String> toReturnAL = new ArrayList<String>();

        NodeList sentences = doc.getChildNodes().item(1).getChildNodes();

        for(int i = 0; i < sentences.getLength(); i++) {
            if(!sentences.item(i).getNodeName().equals("s"))
                continue;
            NodeList tokens = sentences.item(i).getChildNodes();
            for(int j = 0; j < tokens.getLength(); j++) {
                Node tokenNode = tokens.item(j);
                if(tokenNode.getNodeName().equals("toponym")) {
                    toReturnAL.add(tokenNode.getAttributes().getNamedItem("term").getNodeValue());
                }
                else if(tokenNode.getNodeName().equals("w")) {
                    toReturnAL.add(tokenNode.getAttributes().getNamedItem("tok").getNodeValue());
                }

            }
        }

        return toReturnAL.toArray(new String[0]);
        */
    }

    public static String[] getContextWindow(String[] a, int index, int windowSize) {
        ArrayList<String> toReturnAL = new ArrayList<String>();

        int begin = Math.max(0, index - windowSize);
        int end = Math.min(a.length, index + windowSize + 1);

        for(int i = begin; i < end; i++) {
            if(i == index)
                toReturnAL.add("[h]" + a[i] + "[/h]");
            else
                toReturnAL.add(a[i]);
        }

        return toReturnAL.toArray(new String[0]);
    }
}
