public class Flight {

    private String originId;
    private String origin;
    private String destId;
    private String dest;
    private double depDelay;

    public Flight(){

    }

	public Flight(String originId, String origin, String destId, String dest, double depDelay) {
    	super();
		this.originId = originId;
		this.origin = origin;
		this.destId = destId;
		this.dest = dest;
		this.depDelay = depDelay;
	}

	public String getOriginId() {
		return originId;
	}

	public void setOriginId(String originId) {
		this.originId = originId;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDestId() {
		return destId;
	}

	public void setDestId(String destId) {
		this.destId = destId;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public double getDepDelay() {
		return depDelay;
	}

	public void setDepDelay(double depDelay) {
		this.depDelay = depDelay;
	}


	@Override
	public String toString() {
		return "Flight{" +
				"originId='" + originId + '\'' +
				", origin='" + origin + '\'' +
				", destId='" + destId + '\'' +
				", dest='" + dest + '\'' +
				", depDelay=" + depDelay +
				'}';
	}
}

