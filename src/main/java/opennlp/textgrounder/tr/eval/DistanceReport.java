package opennlp.textgrounder.tr.eval;

import java.util.*;

public class DistanceReport {

    private List<Double> distances = new ArrayList<Double>();
    private boolean isSorted = true;

    public void addDistance(double distance) {
        distances.add(distance);
        isSorted = false;
    }

    public double getMeanDistance() {
        if(distances.size() == 0) return -1;

        double total = 0.0;
        for(double distance : distances) {
            total += distance;
        }
        return total / distances.size();
    }

    public double getMedianDistance() {
        if(distances.size() == 0) return -1;
        sort();
        return distances.get(distances.size() / 2);
    }

    public int getNumDistances() {
        return distances.size();
    }

    public double getMinDistance() {
        if(distances.size() == 0) return -1;
        sort();
        return distances.get(0);
    }

    public double getMaxDistance() {
        if(distances.size() == 0) return -1;
        sort();
        return distances.get(distances.size()-1);
    }

    private void sort() {
        if(isSorted)
            return;
        Collections.sort(distances);
        isSorted = true;
    }
}
