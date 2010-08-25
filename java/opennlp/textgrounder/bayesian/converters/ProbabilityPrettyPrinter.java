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
package opennlp.textgrounder.bayesian.converters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.bayesian.apps.*;
import opennlp.textgrounder.bayesian.structs.IntDoublePair;
import opennlp.textgrounder.bayesian.structs.NormalizedProbabilityWrapper;
import opennlp.textgrounder.bayesian.textstructs.Lexicon;
import opennlp.textgrounder.bayesian.topostructs.Region;
import opennlp.textgrounder.bayesian.wrapper.io.*;
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
public class ProbabilityPrettyPrinter {

    /**
     * Hyperparameter for region*doc priors
     */
    protected double alpha;
    /**
     * Hyperparameter for word*region priors
     */
    protected double beta;
    /**
     * Normalization term for word*region gibbs sampler
     */
    protected double betaW;
    /**
     * Number of documents
     */
    protected int D;
    /**
     * Number of tokens
     */
    protected int N;
    /**
     * Number of regions
     */
    protected int R;
    /**
     * Number of non-stopword word types. Equivalent to <p>fW-sW</p>.
     */
    protected int W;
    /**
     *
     */
    protected double[] normalizedRegionCounts;
    /**
     *
     */
    protected double[] normalizedWordByRegionCounts;
    /**
     *
     */
    protected double[] normalizedRegionByDocumentCounts;
    /**
     * 
     */
    protected Lexicon lexicon = null;
    /**
     *
     */
    protected Region[][] regionMatrix = null;
    /**
     *
     */
    protected HashMap<Integer, Region> regionIdToRegionMap;
    /**
     *
     */
    protected ConverterExperimentParameters experimentParameters = null;
    /**
     * 
     */
    protected InputReader inputReader = null;

    public ProbabilityPrettyPrinter(ConverterExperimentParameters _parameters) {
        experimentParameters = _parameters;
        inputReader = new BinaryInputReader(experimentParameters);
    }

    public void readFiles() {
        NormalizedProbabilityWrapper normalizedProbabilityWrapper = inputReader.readProbabilities();

        alpha = normalizedProbabilityWrapper.alpha;
        beta = normalizedProbabilityWrapper.beta;
        betaW = normalizedProbabilityWrapper.betaW;
        D = normalizedProbabilityWrapper.D;
        N = normalizedProbabilityWrapper.N;
        R = normalizedProbabilityWrapper.R;
        W = normalizedProbabilityWrapper.W;
        normalizedRegionByDocumentCounts = normalizedProbabilityWrapper.normalizedRegionByDocumentCounts;
        normalizedRegionCounts = normalizedProbabilityWrapper.normalizedRegionCounts;
        normalizedWordByRegionCounts = normalizedProbabilityWrapper.normalizedWordByRegionCounts;

        lexicon = inputReader.readLexicon();
        regionMatrix = inputReader.readRegions();

        regionIdToRegionMap = new HashMap<Integer, Region>();
        for (Region[] regions : regionMatrix) {
            for (Region region : regions) {
                if (region != null) {
                    regionIdToRegionMap.put(region.id, region);
                }
            }
        }
    }

    public void normalizeAndPrintXMLProbabilities() {
        int outputPerClass = experimentParameters.getOutputPerClass();

        Document doc = new Document();

        String rootName = "probabilities";
        String wordByRegionName = "word-by-region";
        String regionByWordName = "region-by-word";
        String regionByDocumentName = "region-by-document";
        Element root = new Element(rootName);
        doc.addContent(root);

        {
            Element wordByRegionElement = new Element(wordByRegionName);
            root.addContent(wordByRegionElement);

            double sum = 0.;
            for (int i = 0; i < R; ++i) {
                sum += normalizedRegionCounts[i];
            }

            for (int i = 0; i < R; ++i) {
                ArrayList<IntDoublePair> topWords = new ArrayList<IntDoublePair>();
                for (int j = 0; j < W; ++j) {
                    topWords.add(new IntDoublePair(j, normalizedWordByRegionCounts[j * R + i]));
                }
                Collections.sort(topWords);

                Region region = regionIdToRegionMap.get(i);
                Element regionElement = new Element("region");
                wordByRegionElement.addContent(regionElement);

                regionElement.setAttribute("id", String.format("%04d", i));
                regionElement.setAttribute("lat", String.format("%.2f", region.centLat));
                regionElement.setAttribute("lon", String.format("%.2f", region.centLon));
                regionElement.setAttribute("prob", String.format("%.8e", normalizedRegionCounts[i] / sum));

                for (int j = 0; j < outputPerClass; ++j) {
                    Element wordElement = new Element("word");
                    regionElement.addContent(wordElement);

                    IntDoublePair pair = topWords.get(j);
                    wordElement.setAttribute("term", lexicon.getWordForInt(pair.index));
                    wordElement.setAttribute("prob", String.format("%.8e", pair.count / normalizedRegionCounts[i]));
                }
            }
        }

        {
            Element regionByWordElement = new Element(regionByWordName);
            root.addContent(regionByWordElement);

            double[] wordCounts = new double[W];

            for (int i = 0; i < W; ++i) {
                wordCounts[i] = 0;
                int wordoff = i * R;
                for (int j = 0; j < R; ++j) {
                    wordCounts[i] += normalizedWordByRegionCounts[wordoff + j];
                }
            }

            for (int i = 0; i < W; ++i) {
                int wordoff = i * R;
                ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                for (int j = 0; j < R; ++j) {
                    topRegions.add(new IntDoublePair(j, normalizedWordByRegionCounts[wordoff + j]));
                }
                Collections.sort(topRegions);

                Element wordElement = new Element("word");
                regionByWordElement.addContent(wordElement);

                wordElement.setAttribute("term", lexicon.getWordForInt(i));


                for (int j = 0; j < outputPerClass; ++j) {
                    Element regionElement = new Element("region");
                    wordElement.addContent(regionElement);

                    IntDoublePair pair = topRegions.get(j);
                    Region region = regionIdToRegionMap.get(pair.index);
                    regionElement.setAttribute("id", String.format("%04d", pair.index));
                    regionElement.setAttribute("lat", String.format("%.2f", region.centLat));
                    regionElement.setAttribute("lon", String.format("%.2f", region.centLon));
                    regionElement.setAttribute("prob", String.format("%.8e", pair.count / wordCounts[i]));
                }
            }
        }

        {
            Document trdoc = null;
            try {
                trdoc = (new SAXBuilder()).build(experimentParameters.getInputPath());
            } catch (JDOMException ex) {
                Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            } catch (IOException ex) {
                Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            }

            HashMap<Integer, String> docidToName = new HashMap<Integer, String>();
            int docid = 0;
            Element trroot = trdoc.getRootElement();
            ArrayList<Element> documents = new ArrayList<Element>(trroot.getChildren());
            for (Element document : documents) {
                docidToName.put(docid, document.getAttributeValue("id"));
                docid += 1;
            }

            Element regionByDocumentElement = new Element(regionByDocumentName);
            root.addContent(regionByDocumentElement);

            double[] docWordCounts = new double[D];

            for (int i = 0; i < D; ++i) {
                docWordCounts[i] = 0;
                int docoff = i * R;
                for (int j = 0; j < R; ++j) {
                    docWordCounts[i] += normalizedRegionByDocumentCounts[docoff + j];
                }
            }

            for (int i = 0; i < D; ++i) {
                int docoff = i * R;
                ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                for (int j = 0; j < R; ++j) {
                    topRegions.add(new IntDoublePair(j, normalizedRegionByDocumentCounts[docoff + j]));
                }
                Collections.sort(topRegions);

                Element documentElement = new Element("document");
                regionByDocumentElement.addContent(documentElement);

                documentElement.setAttribute("id", docidToName.get(i));

                for (int j = 0; j < outputPerClass; ++j) {
                    Element regionElement = new Element("region");
                    documentElement.addContent(regionElement);

                    IntDoublePair pair = topRegions.get(j);
                    Region region = regionIdToRegionMap.get(pair.index);
                    regionElement.setAttribute("id", String.format("%04d", pair.index));
                    regionElement.setAttribute("lat", String.format("%.2f", region.centLat));
                    regionElement.setAttribute("lon", String.format("%.2f", region.centLon));
                    regionElement.setAttribute("prob", String.format("%.8e", pair.count / docWordCounts[i]));
                }
            }
        }

        try {
            String outputPath = experimentParameters.getXmlConditionalProbabilitiesFilename();
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            xout.output(doc, new FileOutputStream(new File(outputPath)));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * 
     */
    public void normalizeAndPrintWordByRegion() {
        try {
            String wordByRegionFilename = experimentParameters.getWordByRegionProbabilitiesPath();
            BufferedWriter wordByRegionWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(wordByRegionFilename))));

            double sum = 0.;
            for (int i = 0; i < R; ++i) {
                sum += normalizedRegionCounts[i];
            }

            for (int i = 0; i < R; ++i) {
                ArrayList<IntDoublePair> topWords = new ArrayList<IntDoublePair>();
                for (int j = 0; j < W; ++j) {
                    topWords.add(new IntDoublePair(j, normalizedWordByRegionCounts[j * R + i]));
                }
                Collections.sort(topWords);

                Region region = regionIdToRegionMap.get(i);
                wordByRegionWriter.write(String.format("Region%04d\t%.2f\t%.2f\t%.8e", i, region.centLon, region.centLat, normalizedRegionCounts[i] / sum));
                wordByRegionWriter.newLine();
                for (IntDoublePair pair : topWords) {
                    wordByRegionWriter.write(String.format("%s\t%.8e", lexicon.getWordForInt(pair.index), pair.count / normalizedRegionCounts[i]));
                    wordByRegionWriter.newLine();
                }
                wordByRegionWriter.newLine();
            }

            wordByRegionWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    /**
     *
     */
    public void normalizeAndPrintRegionByWord() {
        try {
            String regionByWordFilename = experimentParameters.getRegionByWordProbabilitiesPath();
            BufferedWriter regionByWordWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(regionByWordFilename))));

            double[] wordCounts = new double[W];

            for (int i = 0; i < W; ++i) {
                wordCounts[i] = 0;
                int wordoff = i * R;
                for (int j = 0; j < R; ++j) {
                    wordCounts[i] += normalizedWordByRegionCounts[wordoff + j];
                }
            }

            for (int i = 0; i < W; ++i) {
                int wordoff = i * R;
                ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                for (int j = 0; j < R; ++j) {
                    topRegions.add(new IntDoublePair(j, normalizedWordByRegionCounts[wordoff + j]));
                }
                Collections.sort(topRegions);

                regionByWordWriter.write(String.format("%s", lexicon.getWordForInt(i)));
                regionByWordWriter.newLine();
                for (IntDoublePair pair : topRegions) {
                    Region region = regionIdToRegionMap.get(pair.index);
                    regionByWordWriter.write(String.format("%.2f\t%.2f\t%.8e", region.centLon, region.centLat, pair.count / wordCounts[i]));
                    regionByWordWriter.newLine();
                }
                regionByWordWriter.newLine();
            }

            regionByWordWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    /**
     *
     */
    public void normalizeAndPrintRegionByDocument() {
        try {
            String regionByDocumentFilename = experimentParameters.getRegionByDocumentProbabilitiesPath();
            BufferedWriter regionByDocumentWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(regionByDocumentFilename))));

            SAXBuilder builder = new SAXBuilder();
            Document trdoc = null;
            try {
                trdoc = builder.build(experimentParameters.getInputPath());
            } catch (JDOMException ex) {
                Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            } catch (IOException ex) {
                Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            }

            HashMap<Integer, String> docidToName = new HashMap<Integer, String>();
            int docid = 0;
            Element root = trdoc.getRootElement();
            ArrayList<Element> documents = new ArrayList<Element>(root.getChildren());
            for (Element document : documents) {
                docidToName.put(docid, document.getAttributeValue("id"));
                docid += 1;
            }

            double[] docWordCounts = new double[D];

            for (int i = 0; i < D; ++i) {
                docWordCounts[i] = 0;
                int docoff = i * R;
                for (int j = 0; j < R; ++j) {
                    docWordCounts[i] += normalizedRegionByDocumentCounts[docoff + j];
                }
            }

            for (int i = 0; i < D; ++i) {
                int docoff = i * R;
                ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                for (int j = 0; j < R; ++j) {
                    topRegions.add(new IntDoublePair(j, normalizedRegionByDocumentCounts[docoff + j]));
                }
                Collections.sort(topRegions);

                regionByDocumentWriter.write(String.format("%s", docidToName.get(i)));
                regionByDocumentWriter.newLine();
                for (IntDoublePair pair : topRegions) {
                    Region region = regionIdToRegionMap.get(pair.index);
                    regionByDocumentWriter.write(String.format("%.2f\t%.2f\t%.8e", region.centLon, region.centLat, pair.count / docWordCounts[i]));
                    regionByDocumentWriter.newLine();
                }
                regionByDocumentWriter.newLine();
            }

            regionByDocumentWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
}
