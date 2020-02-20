import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;


public class SparkGraphFrame {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSessionExample")
                .config("spark.sql.warehouse.dir", "/file:C:/temp")
                .config("spark.sql.crossJoin.enabled", true)
                .getOrCreate();


        // -------------------Question-2-------------------
        List<Airport> aList = Utils.readAirportData();
        Dataset<Row> verDF = spark.createDataFrame(aList, Airport.class);

        List<Route> rList = Utils.readRouteData();
        Dataset<Row> edgDF = spark.createDataFrame(rList, Route.class);


        // -------------------Question-3-------------------
        GraphFrame g = new GraphFrame(verDF, edgDF);
        g.persist(StorageLevel.MEMORY_AND_DISK());


        System.out.println("-------------------Question-4-------------------");
        g.edges().show();


        System.out.println("-------------------Question-5-------------------");
        Dataset<Row> degrees = g.degrees();
        degrees.orderBy(functions.col("degree").desc()).show();


        System.out.println("-------------------Question-6-------------------");
        Dataset<Row> inDegrees = g.inDegrees();
        inDegrees.orderBy(functions.col("inDegree").desc()).show();


        System.out.println("-------------------Question-7-------------------");
        Dataset<Row> outDegrees = g.outDegrees();
        outDegrees.orderBy(functions.col("outDegree").desc()).show();


        System.out.println("-------------------Question-8-------------------");
        Dataset<Row> transferts = inDegrees.join(outDegrees, "id").select(
                functions.col("id"),
                functions.col("indegree").divide(functions.col("outdegree")).as("topTransferts")
        );

        transferts.orderBy(functions.abs(functions.col("topTransferts").minus(1))).show();


        System.out.println("-------------------Question-9-------------------");



        System.out.println("-------------------Question-10-------------------");
        System.out.println("Number of Airports: " + g.vertices().count() + "\nNumber of Trips: " + g.edges().count() + "\n");




        System.out.println("-------------------Question-11-1-------------------");
        List<Flight> fList = Utils.readFlightData();
        Dataset<Row> flightDelay = spark.createDataFrame(fList, Flight.class);
        flightDelay.persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> flightDelay111 = flightDelay.filter("origin == 'SFO'").orderBy(functions.col("depDelay").desc());
        flightDelay111.show(false);



        System.out.println("-------------------Question-11-2-------------------");
        Dataset<Row> flightDelay112 = flightDelay.filter("origin = 'SEA'")
                .groupBy("dest")
                .avg("depDelay")
                .filter("avg(depDelay) > 10")
                .orderBy(functions.col("avg(depDelay)").desc());
        flightDelay112.show();



        System.out.println("-------------------Question-12-1-------------------");
        Dataset<Row> flightDelay121 = flightDelay.select(functions.col("originId"),functions.col("origin"), functions.col("destId"), functions.col("dest"), functions.col("depDelay").as("depDelay1"))
                .filter("ORIGIN = 'SFO' AND DEST = 'JAC'")
                .join(flightDelay.select(functions.col("originId"),functions.col("origin"), functions.col("destId"), functions.col("dest"), functions.col("depDelay").as("depDelay2"))
                        .filter("ORIGIN = 'JAC' AND DEST = 'SEA'"));
        flightDelay121.show(false);


        System.out.println("-------------------Question-12-2-------------------");
        Dataset<Row> flightDelay122 = flightDelay121.filter("depDelay1 + depDelay2 < -5");
        flightDelay122.show(false);


        System.out.println("-------------------Question-12-3-------------------");
        Dataset<Row> flightDelay123 = flightDelay.filter("origin = 'SFO'")
                .select("origin", "dest")
                .distinct();
        flightDelay123.show(false);
    }

}
