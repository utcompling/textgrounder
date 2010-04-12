package opennlp.textgrounder.topostructs;

public class Region {

    public double minLon;
    public double maxLon;
    public double minLat;
    public double maxLat;
    /**
     * The longitude center of the region
     */
    public double centLon;
    /**
     * The latitude center of the region
     */
    public double centLat;

    public Region(double minLon, double maxLon, double minLat, double maxLat) {
	this.minLon = minLon;
	this.maxLon = maxLon;
	this.minLat = minLat;
	this.maxLat = maxLat;

        centLon = (maxLon + minLon) / 2;
        centLat = (maxLat + minLat) / 2;
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
