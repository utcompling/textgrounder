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

    public String toKMLSquare(double extent, double height) {
	Coordinate[] square = getContainingSquare(extent);
	return "<coordinates>\n\t\t\t\t\t\t\t\t" 
	    + square[0] + "," + height + "\n\t\t\t\t\t\t\t\t" 
	    + square[1] + "," + height + "\n\t\t\t\t\t\t\t\t" 
	    + square[2] + "," + height + "\n\t\t\t\t\t\t\t\t" 
	    + square[2] + "," + height + "\n\t\t\t\t\t\t\t\t" 
	    + square[3] + "," + height + "\n\t\t\t\t\t\t\t\t" 
	    + square[0] + "," + height + "\n\t\t\t\t\t\t\t</coordinates>";
    }
    
    public String toString() {
	return latitude + "," + longitude;
    }
}
