import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {


    public static List<Airport> readAirportData(){
        FileReader f = null;
        List<Airport> list = new ArrayList<>();
        try {
            f = new FileReader("airports.dat");
            BufferedReader br = new BufferedReader(f);

            String line;
            while((line = br.readLine()) != null) {
                String[] lineTab = line.split(",");
                Airport a = new Airport(lineTab[0], lineTab[1], lineTab[2], lineTab[3], lineTab[4], lineTab[5]);
                list.add(a);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*for(Airport a: list){
            System.out.println(a);
        }*/
        return list;
    }


    public static List<Route> readRouteData(){
        FileReader f = null;
        List<Route> list = new ArrayList<>();
        try {
            f = new FileReader("routes.dat");
            BufferedReader br = new BufferedReader(f);

            String line;
            while((line = br.readLine()) != null) {
                String[] lineTab = line.split(",");
                Route r = new Route(lineTab[0], lineTab[1], lineTab[2], lineTab[3], lineTab[4], lineTab[5]);
                list.add(r);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*for(Route r: list){
            System.out.println(r);
        }*/
        return list;
    }

    public static List<Flight> readFlightData(){
        FileReader f = null;
        List<Flight> list = new ArrayList<>();
        try {
            f = new FileReader("flights.csv");
            BufferedReader br = new BufferedReader(f);

            String line, line2;
            line = br.readLine();
            while((line = br.readLine()) != null) {
                line2 = line.replace("\"", "");
                String[] lineTab = line2.split(",");
                if (lineTab.length>=5) {
                    Flight a = new Flight(lineTab[0], lineTab[1], lineTab[2], lineTab[3], Double.parseDouble(lineTab[4]));
                    list.add(a);
                }else {
                    Flight a = new Flight(lineTab[0], lineTab[1], lineTab[2], lineTab[3], 0.0);
                    list.add(a);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*for(Flight a: list){
            System.out.println(a);
        }*/
        return list;
    }




}
