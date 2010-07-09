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
package opennlp.rlda.apps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ExperimentParametersLoader {

    static ExperimentParameters loadParameters(String _path) {
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
            Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ExperimentParametersLoader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return experimentParameters;
    }
}
