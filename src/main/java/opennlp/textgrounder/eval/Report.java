///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.eval;

public class Report {

    private int tp;
    private int fp;
    private int fn;
    private int totalInstances;

    public int getFN() {
        return fn;
    }

    public int getFP() {
        return fp;
    }

    public int getTP() {
        return tp;
    }

    public int getInstanceCount() {
        return totalInstances;
    }

    public void incrementTP() {
        tp++;
        totalInstances++;
    }

    public void incrementFP() {
        fp++;
        totalInstances++;
    }

    public void incrementFN() {
        fn++;
        totalInstances++;
    }

    public void incrementInstanceCount() {
        totalInstances++;
    }

    public double getAccuracy() {
        return (double) tp / totalInstances;
    }

    public double getPrecision() {
        return (double) tp / (tp + fp);
    }

    public double getRecall() {
        return (double) tp / (tp + fn);
    }

    public double getFScore() {
        double p = getPrecision();
        double r = getRecall();
        return (2 * p * r) / (p + r);
    }
}

