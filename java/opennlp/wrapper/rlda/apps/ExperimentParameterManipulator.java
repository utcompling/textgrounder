///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Taesun Moon <tsunmoon@gmail.com>.
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.wrapper.rlda.apps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ExperimentParameterManipulator {

    public static ExperimentParameters loadParameters(String _path) {
        ExperimentParameters experimentParameters = new ExperimentParameters();

        File file = new File(_path);

        Document doc = null;
        SAXBuilder builder = new SAXBuilder();
        try {
            doc = builder.build(file);
            Element root = doc.getRootElement();
            ArrayList<Element> params = new ArrayList<Element>(root.getChild("ExperimentStage").getChildren());
            for (Element param : params) {
                try {
                    String fieldName = param.getName();
                    String fieldValue = param.getValue();
                    String fieldTypeName = param.getAttributeValue("type");
                    if (fieldTypeName.equals("int")) {
                        int value = Integer.parseInt(fieldValue);
                        experimentParameters.getClass().getDeclaredField(fieldName).setInt(experimentParameters, value);
                    } else if (fieldTypeName.equals("double")) {
                        double value = Double.parseDouble(fieldValue);
                        experimentParameters.getClass().getDeclaredField(fieldName).setDouble(experimentParameters, value);
                    } else if (fieldTypeName.equals("string")) {
                        experimentParameters.getClass().getDeclaredField(fieldName).set(experimentParameters, fieldValue);
                    } else {
                        continue;
                    }

                } catch (NoSuchFieldException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                } catch (SecurityException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                } catch (IllegalArgumentException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                } catch (IllegalAccessException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                }
            }

        } catch (JDOMException ex) {
            Logger.getLogger(ExperimentParameterManipulator.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ExperimentParameterManipulator.class.getName()).log(Level.SEVERE, null, ex);
        }

        return experimentParameters;
    }

    public static void dumpFieldsToXML(ExperimentParameters _experimentParameters, String _outputPath,
          String _stageName) {
        Document doc = new Document();
        Element stage = new Element(_stageName);
        doc.addContent(stage);

        try {
            for (Field field : _experimentParameters.getClass().getDeclaredFields()) {
                String fieldName = field.getName();
                String fieldValue = "";
                Object o = field.getType();
                String fieldTypeName = field.getType().getName();
                if (fieldTypeName.equals("int")) {
                    fieldValue = String.format("%d", field.getInt(_experimentParameters));
                } else if (fieldTypeName.equals("double")) {
                    fieldValue = String.format("%f", field.getDouble(_experimentParameters));
                } else if (fieldTypeName.equals("java.lang.String")) {
                    String s = (String) field.get(_experimentParameters);
                    fieldValue = s;
                    fieldTypeName = "string";
                } else {
                    continue;
                }

                Element element = new Element(fieldName);
                element.setText(fieldValue);
                element.setAttribute("type", fieldTypeName);
                stage.addContent(element);
            }
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(ExperimentParameterManipulator.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(ExperimentParameterManipulator.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        serialize(doc, new File(_outputPath));
    }

    static void serialize(Document doc,
          File _file) {
        try {
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            xout.output(doc, new FileOutputStream(_file));
        } catch (IOException ex) {
            Logger.getLogger(ExperimentParameterManipulator.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
}
