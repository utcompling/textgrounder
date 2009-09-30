package opennlp.textgrounder.geo;

public class Coordinate {
    public double longitude;
    public double latitude;
    
    public Coordinate(double lon, double lat) {
	longitude = lon;
	latitude = lat;
    }

    public Coordinate[] getContainingSquare(double extent) {
	Coordinate[] square =  {
	    new Coordinate(longitude - extent, latitude + extent),
	    new Coordinate(longitude + extent, latitude + extent),
	    new Coordinate(longitude + extent, latitude - extent),
	    new Coordinate(longitude - extent, latitude - extent)
	};
	return square;
    }
    
    public String toString() {
	return latitude + "," + longitude;
    }
}
