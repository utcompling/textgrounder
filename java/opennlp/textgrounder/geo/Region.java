package opennlp.textgrounder.geo;

public class Region {

    double minLon;
    double maxLon;
    double minLat;
    double maxLat;

    public Region(double minLon, double maxLon, double minLat, double maxLat) {
	this.minLon = minLon;
	this.maxLon = maxLon;
	this.minLat = minLat;
	this.maxLat = maxLat;
    }

    public boolean contains(Coordinate coord) {
	if(coord.longitude >= minLon && coord.longitude <= maxLon
	   && coord.latitude >= minLat && coord.latitude <= maxLat)
	    return true;
	return false;
    }

    public boolean contains(double lon, double lat) {
	return contains(new Coordinate(lon, lat));
    }

}
