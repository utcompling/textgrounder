package opennlp.textgrounder.topostructs;

public class Coordinate {
    private final static double twoPI = 2*Math.PI;

    public double longitude;
    public double latitude;
    
    public Coordinate(double lon, double lat) {
	longitude = lon;
	latitude = lat;
    }

    public String toKMLPolygon(int sides, double radius, double height) {
	final double radianUnit = twoPI/sides;
	final double startRadian = radianUnit/2;
	double currentRadian = startRadian;

	StringBuilder sb = new StringBuilder("<coordinates>\n\t\t\t\t\t\t\t\t");
	
	while (currentRadian <= twoPI+startRadian) {
	    sb.append(latitude+radius*Math.cos(currentRadian)).append(",").append(longitude+radius*Math.sin(currentRadian)).append(",").append(height).append("\n\t\t\t\t\t\t\t\t");
	    currentRadian += radianUnit;
	}
	sb.append("</coordinates>");

	return sb.toString();
    }

    public Coordinate getNthSpiralPoint(int n, double initRadius) {
	if(n == 0)
	  return this;
	
	final double radianUnit = twoPI/10;
	
	double radius = initRadius + (initRadius * .1) * n;

	double angle = radianUnit/2 + n * radianUnit;

	return new Coordinate(this.longitude + radius * Math.sin(angle), this.latitude + radius * Math.cos(angle));
    }

    public String toString() {
	return latitude + "," + longitude;
    }
}
