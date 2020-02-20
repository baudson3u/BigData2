public class Route {


    private String airline;
    private String airlineId;
    private String sourceAirport;
    private String src;
    private String destAirport;
    private String dst;

    public Route(){

    }

    public Route(String airline, String airlineId, String sourceAirport, String src, String destAirport, String dst) {
        this.airline = airline;
        this.airlineId = airlineId;
        this.sourceAirport = sourceAirport;
        this.src = src;
        this.destAirport = destAirport;
        this.dst = dst;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public String getAirlineId() {
        return airlineId;
    }

    public void setAirlineId(String airlineId) {
        this.airlineId = airlineId;
    }

    public String getSourceAirport() {
        return sourceAirport;
    }

    public void setSourceAirport(String sourceAirport) {
        this.sourceAirport = sourceAirport;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDestAirport() {
        return destAirport;
    }

    public void setDestAirport(String destAirport) {
        this.destAirport = destAirport;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    @Override
    public String toString() {
        return "Route{" +
                "airline='" + airline + '\'' +
                ", airlineId='" + airlineId + '\'' +
                ", sourceAirport='" + sourceAirport + '\'' +
                ", src='" + src + '\'' +
                ", destAirport='" + destAirport + '\'' +
                ", dst='" + dst + '\'' +
                '}';
    }
}
