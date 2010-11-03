/*
 * This is the version of Model that reads in XML format that matches the TR-CoNLL XML format
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
 *
 * This class is a superclass to e.g. RandomModelXML and BasicMinDistanceXML.
 */

package opennlp.textgrounder.old.models;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

import opennlp.textgrounder.util.*;

public abstract class ModelXML {

    protected static final int CONTEXT_WINDOW_SIZE = 20;

    protected static DocumentBuilderFactory dbf;
    protected static DocumentBuilder db;

    /*public static void main(String[] args) throws Exception {
        ModelXML modelXML = new ModelXML(args[0], args[1]);
    }*/

    protected ModelXML() {
        
    }

    public ModelXML(String inputXMLPath, String outputXMLPath) throws Exception {
        dbf = DocumentBuilderFactory.newInstance();
        db = dbf.newDocumentBuilder();

        disambiguateToponyms(inputXMLPath, outputXMLPath);
    }
    
    abstract protected void disambiguateToponyms(String inputXMLPath, String outputXMLPath) throws Exception;

}
