# Projet BigData : BAUDSON / BACQ

## Questions :


### 1. Import graphframes (from Spark-Packages)
```java
//QUESTION-1
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
```
### 2. Create Vertices (airports) and Edges (flights (routes))	

On crée les vertices et les edges en utilisant des structures de données, Airport et Route.

**Exemple de structure :**

```java
public class Airport {  
  
	private String id;  
	private String name;  
	private String city;  
	private String country;  
	private String iata;  
	private String icao;  

	public Airport(){  
	  
	}  
  
  
	public Airport(String id, String name, String city, String country, String iata, String icao) {  
		super();  
		this.id = id;  
		this.name = name;  
		this.city = city;  
		this.country = country;  
		this.iata = iata;  
		this.icao = icao;  
	}

	...
}
```

On crée donc les vertices et edges en utilisant ces structures.

```java
// -------------------Question-2-------------------
List<Airport> aList = Utils._readAirportData_();
Dataset<Row> verDF = spark.createDataFrame(aList, Airport.class);

List<Route> rList = Utils._readRouteData_();
Dataset<Row> edgDF = spark.createDataFrame(rList, Route.class);
```

### 3. Create the graph

On construit le graphe avec les vertices et les edges créés précédemment.

```java
// -------------------Question-3-------------------
GraphFrame g = new GraphFrame(verDF, edgDF);
g.persist(StorageLevel._MEMORY_AND_DISK_());
```

### 4. Display the edges

```java
System._out_.println("-------------------Question-4-------------------");
g.edges().show();
```

Résultat :

```
+-------+---------+-----------+----+-------------+----+
|airline|airlineId|destAirport| dst|sourceAirport| src|
+-------+---------+-----------+----+-------------+----+
|     2B|      410|        KZN|2990|          AER|2965|
|     2B|      410|        KZN|2990|          ASF|2966|
|     2B|      410|        MRV|2962|          ASF|2966|
|     2B|      410|        KZN|2990|          CEK|2968|
|     2B|      410|        OVB|4078|          CEK|2968|
|     2B|      410|        KZN|2990|          DME|4029|
|     2B|      410|        NBC|6969|          DME|4029|
|     2B|      410|        TGK|  \N|          DME|4029|
|     2B|      410|        UUA|6160|          DME|4029|
|     2B|      410|        KGD|2952|          EGO|6156|
|     2B|      410|        KZN|2990|          EGO|6156|
|     2B|      410|        NBC|6969|          GYD|2922|
|     2B|      410|        EGO|6156|          KGD|2952|
|     2B|      410|        AER|2965|          KZN|2990|
|     2B|      410|        ASF|2966|          KZN|2990|
|     2B|      410|        CEK|2968|          KZN|2990|
|     2B|      410|        DME|4029|          KZN|2990|
|     2B|      410|        EGO|6156|          KZN|2990|
|     2B|      410|        LED|2948|          KZN|2990|
|     2B|      410|        SVX|2975|          KZN|2990|
+-------+---------+-----------+----+-------------+----+
only showing top 20 rows
```

### 5. Sort and display the degree of each vertex.

Pour trier les données on utilise .orderBy( ).

```java
System._out_.println("-------------------Question-5-------------------");
Dataset<Row> degrees = g.degrees();
degrees.orderBy(functions._col_("degree").desc()).show();//(int)degrees.count());
```


Résultat :

```
+----+------+
|  id|degree|
+----+------+
|3682|  1826|
|3830|  1108|
|3364|  1069|
| 507|  1051|
|1382|  1041|
| 340|   990|
|3484|   990|
|3670|   936|
|3797|   911|
| 580|   903|
|3406|   825|
|3316|   820|
|1218|   783|
|3930|   740|
|3751|   735|
|3576|   734|
| 346|   728|
|1701|   719|
|2188|   710|
|3077|   710|
+----+------+
only showing top 20 rows
```

Pour afficher toutes les lignes, il suffit de remplacer cette ligne :

```java
degrees.orderBy(functions._col_("degree").desc()).show();//(int)degrees.count());
```

Par celle la :

```java
degrees.orderBy(functions._col_("degree").desc()).show((int)degrees.count());
```



### 6. Sort and display the indegree (the number of flights to the airport) of each vertex


```java
System._out_.println("-------------------Question-6-------------------");
Dataset<Row> inDegrees = g.inDegrees();
inDegrees.orderBy(functions._col_("inDegree").desc()).show();
```

Résultat :

```
+----+--------+
|  id|inDegree|
+----+--------+
|3682|     911|
|3830|     550|
|3364|     534|
| 507|     524|
|1382|     517|
|3484|     498|
| 340|     493|
|3670|     467|
|3797|     455|
| 580|     450|
|3406|     414|
|3316|     412|
|1218|     392|
|3751|     374|
|3930|     370|
|3576|     366|
|1701|     361|
| 346|     360|
|3077|     355|
|2188|     354|
+----+--------+
only showing top 20 rows
```

### 7. Sort and display the outdegree (the number of flights leaving the airport) of each vertex

```java
System._out_.println("-------------------Question-7-------------------");
Dataset<Row> outDegrees = g.outDegrees();
outDegrees.orderBy(functions._col_("outDegree").desc()).show();
```

Résultat :

```
+----+---------+
|  id|outDegree|
+----+---------+
|3682|      915|
|3830|      558|
|3364|      535|
| 507|      527|
|1382|      524|
| 340|      497|
|3484|      492|
|3670|      469|
|3797|      456|
| 580|      453|
|3406|      411|
|3316|      408|
|1218|      391|
|3930|      370|
| 346|      368|
|3576|      368|
|3751|      361|
|1701|      358|
| 502|      356|
|2188|      356|
+----+---------+
only showing top 20 rows
```

### 8. Determine the top transfer airports. An easy way to calculate this is by calculating the ratio of inDegrees / outDegrees . Values close to 1 may indicate many transfers, whereas values <1 indicate many outgoing flights and values >1 indicate many incoming flights.

Dans un premier temps nous unissons les deux informations en divisant  les entrées par les sorties. Puis ensuite nous trions les valeurs en les comparant à 0, pour obtenir une fonction absolu pertinente.  

```java
System._out_.println("-------------------Question-8-------------------");
Dataset<Row> transferts = inDegrees.join(outDegrees, "id").select(
	functions._col_("id"),
	functions._col_("indegree").divide(functions._col_("outdegree")).as("topTransferts")
);

transferts.orderBy(
	functions._abs_(functions._col_("topTransferts").minus(1))
).show();
```

Résultat :

```
+----+-------------+
|  id|topTransferts|
+----+-------------+
|2756|          1.0|
|3517|          1.0|
|3414|          1.0|
|2275|          1.0|
|3959|          1.0|
|2162|          1.0|
| 691|          1.0|
| 467|          1.0|
|1512|          1.0|
|2069|          1.0|
|2294|          1.0|
|7252|          1.0|
|6240|          1.0|
|5925|          1.0|
|6194|          1.0|
|5645|          1.0|
|2136|          1.0|
| 675|          1.0|
|6731|          1.0|
|3368|          1.0|
+----+-------------+
only showing top 20 rows
```

### 9. Compute the triplets

 La question n'étant pas claire, nous n'avons ni compris l'opération à effectuer ni le résultat attendu.
 
### 10. Print the number of airports and trips of your graph

```java
System._out_.println("-------------------Question-10-------------------");
System._out_.println("Number of Airports: " + g.vertices().count() + "\nNumber of Trips: " + g.edges().count() + "\n");
```

Résultat :

```
Number of Airports: 7698
Number of Trips: 67663
```

### 11. Create the following query :

#### 11.1 What flights departing from SFO are most likely to have significant delays? (Sorting the results based on the delay)

```java
System._out_.println("-------------------Question-11-1-------------------");
List<Flight> fList = Utils._readFlightData_();
Dataset<Row> flightDelay = spark.createDataFrame(fList, Flight.class);
flightDelay.persist(StorageLevel._MEMORY_AND_DISK_());

Dataset<Row> flightDelay111 = flightDelay.filter("origin == 'SFO'").orderBy(functions._col_("depDelay").desc());
flightDelay111.show(false);
```

Le **show(false)** sert à ne pas tronquer le nombre de colonnes.

Résultat :

```
+--------+----+------+------+--------+
|depDelay|dest|destId|origin|originId|
+--------+----+------+------+--------+
|1126.0  |ASE |10372 |SFO   |14771   |
|1072.0  |ORD |13930 |SFO   |14771   |
|913.0   |MFR |13264 |SFO   |14771   |
|885.0   |DEN |11292 |SFO   |14771   |
|510.0   |STS |15023 |SFO   |14771   |
|499.0   |FAT |11638 |SFO   |14771   |
|474.0   |DFW |11298 |SFO   |14771   |
|451.0   |JFK |12478 |SFO   |14771   |
|412.0   |LGB |12954 |SFO   |14771   |
|400.0   |SNA |14908 |SFO   |14771   |
|383.0   |LGB |12954 |SFO   |14771   |
|380.0   |JFK |12478 |SFO   |14771   |
|349.0   |JFK |12478 |SFO   |14771   |
|335.0   |SLC |14869 |SFO   |14771   |
|316.0   |JFK |12478 |SFO   |14771   |
|311.0   |LIH |12982 |SFO   |14771   |
|311.0   |MSP |13487 |SFO   |14771   |
|303.0   |SBP |14698 |SFO   |14771   |
|291.0   |SMF |14893 |SFO   |14771   |
|290.0   |HNL |12173 |SFO   |14771   |
+--------+----+------+------+--------+
only showing top 20 rows
```

#### 11.2 What destination states tend to have significant delays departing from SEA? (delay > 10)

```java
System._out_.println("-------------------Question-11-2-------------------");
Dataset<Row> flightDelay112 = flightDelay.filter("origin = 'SEA'")
	.groupBy("dest")
	.avg("depDelay")
	.filter("avg(depDelay) > 10")
	.orderBy(functions._col_("avg(depDelay)").desc());
flightDelay112.show();
```

Résultat :

```
+----+------------------+
|dest|     avg(depDelay)|
+----+------------------+
| JAC|              19.0|
| MIA|              17.5|
| BWI| 15.73913043478261|
| LGB|14.564516129032258|
| FAT| 13.64516129032258|
| BOS|11.343283582089553|
| OGG| 11.07258064516129|
| OKC|10.774193548387096|
+----+------------------+
```


### 12. Create the following query :

#### 12.1 Query 1: SFO->JAC->SEA

```java
System._out_.println("-------------------Question-12-1-------------------");

Dataset<Row> flightDelay121 = flightDelay.select(functions._col_("originId"),functions._col_("origin"), functions._col_("destId"), functions._col_("dest"), functions._col_("depDelay").as("depDelay1"))
	.filter("ORIGIN = 'SFO' AND DEST = 'JAC'"
	.join(flightDelay.select(functions._col_("originId"),functions._col_("origin"), functions._col_("destId"), functions._col_("dest"), functions._col_("depDelay").as("depDelay2"))
	.filter("ORIGIN = 'JAC' AND DEST = 'SEA'"));
flightDelay121.show(false);
```

Résultat :

```
+--------+------+------+----+---------+--------+------+------+----+---------+
|originId|origin|destId|dest|depDelay1|originId|origin|destId|dest|depDelay2|
+--------+------+------+----+---------+--------+------+------+----+---------+
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |11.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |173.0    |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-3.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-4.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-5.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-2.0     |
|14771   |SFO   |12441 |JAC |19.0     |12441   |JAC   |14747 |SEA |11.0     |
|14771   |SFO   |12441 |JAC |19.0     |12441   |JAC   |14747 |SEA |173.0    |
|14771   |SFO   |12441 |JAC |19.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |19.0     |12441   |JAC   |14747 |SEA |-3.0     |
|14771   |SFO   |12441 |JAC |19.0     |12441   |JAC   |14747 |SEA |-4.0     |
|14771   |SFO   |12441 |JAC |19.0     |12441   |JAC   |14747 |SEA |-5.0     |
|14771   |SFO   |12441 |JAC |19.0     |12441   |JAC   |14747 |SEA |-2.0     |
|14771   |SFO   |12441 |JAC |17.0     |12441   |JAC   |14747 |SEA |11.0     |
|14771   |SFO   |12441 |JAC |17.0     |12441   |JAC   |14747 |SEA |173.0    |
|14771   |SFO   |12441 |JAC |17.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |17.0     |12441   |JAC   |14747 |SEA |-3.0     |
|14771   |SFO   |12441 |JAC |17.0     |12441   |JAC   |14747 |SEA |-4.0     |
|14771   |SFO   |12441 |JAC |17.0     |12441   |JAC   |14747 |SEA |-5.0     |
+--------+------+------+----+---------+--------+------+------+----+---------+
only showing top 20 rows
```

#### 12.2 Query2: filter query 1 where delay < - 5. delay=DEP_DELAY

```java
System._out_.println("-------------------Question-12-2-------------------");
Dataset<Row> flightDelay122 = flightDelay121.filter("depDelay1 + depDelay2 < -5");
flightDelay122.show(false);
```

Résultat :

```
+--------+------+------+----+---------+--------+------+------+----+---------+
|originId|origin|destId|dest|depDelay1|originId|origin|destId|dest|depDelay2|
+--------+------+------+----+---------+--------+------+------+----+---------+
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-3.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-4.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-5.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-3.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-4.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-5.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-2.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-3.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-4.0     |
|14771   |SFO   |12441 |JAC |-3.0     |12441   |JAC   |14747 |SEA |-5.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-3.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-4.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-5.0     |
|14771   |SFO   |12441 |JAC |-8.0     |12441   |JAC   |14747 |SEA |-2.0     |
|14771   |SFO   |12441 |JAC |-7.0     |12441   |JAC   |14747 |SEA |-8.0     |
|14771   |SFO   |12441 |JAC |-7.0     |12441   |JAC   |14747 |SEA |-3.0     |
+--------+------+------+----+---------+--------+------+------+----+---------+
only showing top 20 rows
```

#### 12.3 Query 3: Extract the destinations from SFO

```java
  
System._out_.println("-------------------Question-12-3-------------------");
Dataset<Row> flightDelay123 = flightDelay.filter("origin = 'SFO'")
	.select("origin", "dest")
	.distinct();
flightDelay123.show(false);	
```

Résultat :

```
+------+----+
|origin|dest|
+------+----+
|SFO   |TUS |
|SFO   |BOI |
|SFO   |MSY |
|SFO   |OMA |
|SFO   |STL |
|SFO   |HDN |
|SFO   |SMF |
|SFO   |MRY |
|SFO   |GEG |
|SFO   |STS |
|SFO   |EUG |
|SFO   |PIT |
|SFO   |MCI |
|SFO   |ASE |
|SFO   |RDD |
|SFO   |ANC |
|SFO   |AUS |
|SFO   |ORD |
|SFO   |CLT |
|SFO   |BNA |
+------+----+
only showing top 20 rows
```
