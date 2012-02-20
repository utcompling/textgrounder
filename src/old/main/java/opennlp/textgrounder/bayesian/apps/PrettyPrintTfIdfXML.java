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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import opennlp.textgrounder.bayesian.converters.ProbabilityPrettyPrinterRLDA;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class PrettyPrintTfIdfXML extends BaseApp {

    public static void main(String[] args) throws IOException, FileNotFoundException, XMLStreamException {

        ConverterExperimentParameters experimentParameters = new ConverterExperimentParameters();
        processRawCommandline(args, experimentParameters);

        ProbabilityPrettyPrinterRLDA probabilityPrettyPrinter = new ProbabilityPrettyPrinterRLDA(experimentParameters);
        probabilityPrettyPrinter.readFiles();
        probabilityPrettyPrinter.normalizeAndPrintXMLProbabilities();

        String outputPath = experimentParameters.getXmlConditionalProbabilitiesPath();
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        XMLStreamWriter w = factory.createXMLStreamWriter(new BufferedWriter(new FileWriter(outputPath)));

        probabilityPrettyPrinter.writeTfIdfWords(w);
    }
}

