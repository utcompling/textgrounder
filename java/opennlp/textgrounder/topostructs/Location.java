package opennlp.textgrounder.topostructs;

import java.util.*;

public class Location {

    public int id;
    public String name;
    public String type;
    public Coordinate coord;
    public int pop;
    public String container;

    /**
     * List of pack pointers into the DocumentSet so that context (snippets) can be extracted
     */
    public ArrayList<Integer> backPointers;

    /**
     * Counts of location in given text. Is double type to accomodate
     * hyperparameters and fractional counts;
     */
    public double count;

    public Location(int id, String name, String type, Coordinate coord, int pop,
		    String container, int count) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.coord = coord;
        this.pop = pop;
        this.container = container;
        this.count = count;
    }

    /*    public Location(int id, String name, String type, double lon, double lat, int pop, String container, int count) {
    Location(id, name, type, new Coordinate(lon, lat), pop, container, count);
    }*/
    @Override
    public String toString() {
        return id + ", " + name + ", " + type + ", (" + coord + "), " + pop + ", " + container;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Location other = (Location) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }
}
