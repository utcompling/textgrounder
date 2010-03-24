package opennlp.textgrounder.topostructs;

public class Location {

    public int id;
    public String name;
    public String type;
    public Coordinate coord;
    public int pop;
    public String container;
    public int count;

    public Location(int id, String name, String type, Coordinate coord, int pop, String container, int count) {
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

    public String toString() {
	return id + ", " + name + ", " + type + ", (" + coord + "), " + pop + ", " + container;
    }

}
