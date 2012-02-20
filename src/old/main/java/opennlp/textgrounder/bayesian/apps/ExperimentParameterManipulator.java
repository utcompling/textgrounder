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
package opennlp.textgrounder.bayesian.apps;

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

    public static void loadParameters(Object _experimentParameters, String _path,
          String _stageName) {

        File file = new File(_path);

        Document doc = null;
        SAXBuilder builder = new SAXBuilder();
        try {
            doc = builder.build(file);
            Element root = doc.getRootElement();
            ArrayList<Element> params = new ArrayList<Element>(root.getChild(_stageName).getChildren());
            for (Element param : params) {
                try {
                    String fieldName = param.getName();
                    String fieldValue = param.getValue();
                    String fieldTypeName = param.getAttributeValue("type");
                    recursiveFind(_experimentParameters.getClass(), _experimentParameters, fieldName, fieldTypeName, fieldValue);
                } catch (NoSuchFieldException ex) {
                    System.err.println(ex.getMessage() + " is not a relevant field");
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
    }

    public static void dumpFieldsToXML(Object _experimentParameters,
          String _outputPath,
          String _stageName) {
        Document doc = new Document();
        Element root = new Element("experiments");
        doc.addContent(root);
        Element stage = new Element(_stageName);
        root.addContent(stage);

        try {
            for (Field field :
                  _experimentParameters.getClass().getDeclaredFields()) {
                String fieldName = field.getName();
                String fieldValue = "";
                Object o = field.getType();
                String fieldTypeName = field.getType().getName();
                if (fieldTypeName.equals("int")) {
                    fieldValue = String.format("%d", field.getInt(_experimentParameters));
                } else if (fieldTypeName.equals("double")) {
                    fieldValue = String.format("%f", field.getDouble(_experimentParameters));
                } else if (fieldTypeName.equals("boolean")) {
                    fieldValue = Boolean.toString(field.getBoolean(_experimentParameters));
                } else if (fieldTypeName.equals("java.lang.String")) {
                    String s = (String) field.get(_experimentParameters);
                    fieldValue = s;
                    fieldTypeName = "string";
                } else if (fieldTypeName.endsWith("java.lang.Enum")) {
                    String s = field.get(_experimentParameters).toString();
                    fieldValue = s;
                    fieldTypeName = "enum";
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

    public static void serialize(Document doc,
          File _file) {
        try {
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            xout.output(doc, new FileOutputStream(_file));
        } catch (IOException ex) {
            Logger.getLogger(ExperimentParameterManipulator.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    protected static void recursiveFind(Class _class,
          Object _experimentParameters, String _fieldName, String _fieldTypeName,
          String _fieldValue) throws IllegalArgumentException,
          IllegalAccessException, NoSuchFieldException {

        try {
            if (_fieldTypeName.equals("int")) {
                int value = Integer.parseInt(_fieldValue);
                _class.getDeclaredField(_fieldName).setInt(_experimentParameters, value);
            } else if (_fieldTypeName.equals("double")) {
                double value = Double.parseDouble(_fieldValue);
                _class.getDeclaredField(_fieldName).setDouble(_experimentParameters, value);
            } else if (_fieldTypeName.equals("boolean")) {
                boolean value = Boolean.parseBoolean(_fieldValue);
                _class.getDeclaredField(_fieldName).setBoolean(_experimentParameters, value);
            } else if (_fieldTypeName.equals("string")) {
                _class.getDeclaredField(_fieldName).set(_experimentParameters, _fieldValue);
            } else if (_fieldTypeName.equals("enum")) {
                Enum classDefaultVal = (Enum) _class.getDeclaredField(_fieldName).get(_experimentParameters);
                Enum[] enumValues = classDefaultVal.getClass().getEnumConstants();
                for (Enum e : enumValues) {
                    if (e.toString().equalsIgnoreCase(_fieldValue)) {
                        _class.getDeclaredField(_fieldName).set(_experimentParameters, e);
                        break;
                    }
                }
            } else {
                return;
            }
        } catch (NoSuchFieldException ex) {
            if (_class.getSuperclass() != null) {
                recursiveFind(_class.getSuperclass(), _experimentParameters, _fieldName, _fieldTypeName, _fieldValue);
            } else {
                throw new NoSuchFieldException(ex.getMessage());
            }
        }
    }

    /**
     * Function serving no use. Just keeping it around as a reminder of how
     * to construct generic functions
     *
     * @param <T>
     * @param _inputE
     * @param _inputS
     * @return
     */
    protected static <T extends Enum<T>> T goThroughEnum(Enum<T> _inputE, String _inputS) {
        T x = (T) _inputE;
        return x;
    }
}
