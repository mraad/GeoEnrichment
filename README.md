GeoEnrichment
=============

Hadoop MapReduce job to perform GeoEnrichment on BigData.

What is GeoEnrichment? An example would best describe it. Given a big set of customer location records,
I would like each location to be GeoEnriched with the average income of the zip code where that location falls into and with the
number of people between the age of 25 and 30 that live in that zip code.

Before GeoEnrichment:
<table>
<tr>
 <td>CUSTID</td>
 <td>LAT</td>
 <td>LON</td>
</tr>
</table>

After GeoEnhancement:
<table>
<tr>
 <td>CUSTID</td>
 <td>LAT</td>
 <td>LON</td>
 <td>AVERAGE_INCOME</td>
 <td>AGE_25_30</td>
</tr>
</table>

Of course the key to this whole thing is the spatial reference data :-)

I've implemented two search methods:

* Point-In-Polygon method
* Nearest Neighbor Weighted method

The Point-In-Polygon (PiP) method is fairly simple. Given a point, find the polygon it falls into and pull from the
polygon feature the selected attributes and add them to the original point.

The Nearest Neighbor Weighted (NNW) method finds all the reference points within a specified distance and weights each point based
on its distance. The GeoEnrichment value is the sum of the weighted attribute value.

![w_{i}=1-\frac{d_{i}}{R}](http://latex.codecogs.com/gif.latex?w_%7Bi%7D%3D1-%5Cfrac%7Bd_%7Bi%7D%7D%7BR%7D)

Where:

* w is weight of the reference point
* d is the distance between the reference point from the search point
* R is the search radius

![V_{attr}=\frac{\sum_1^nw_{i}v_{i}}{n}](http://latex.codecogs.com/gif.latex?V_%7Battr%7D%3D%5Cfrac%7B%5Csum_1%5Enw_%7Bi%7Dv_%7Bi%7D%7D%7Bn%7D)

### Dependencies

    $ git clone https://github.com/kungfoo/geohash-java.git
    $ mvn install

    $ git clone https://github.com/Esri/geometry-api-java.git
    $ mvn install

## Build and package

**Note: In my dev environment, I had to symbolic soft link java to /bin/java to pass the maven test phase:**

    $ sudo ln -s /usr/bin/java /bin/java (optional, depending on your dev environment)

    $ mvn package

## Sample Data
The following will generate 1,000,000 points for GeoEnrichment:

    $ hadoop fs -rm -skipTrash data.tsv
    $ awk -f data.awk | hadoop fs -put - data.tsv

Use the world countries for GeoEnrichment. Put the zip file (included in the data folder) that contains the world countries in shapefile format in HDFS:

    $ hadoop fs -put cntry06.zip cntry06.zip

## GeoEnrichment In Action

    $ hadoop fs -rm -R -skipTrash output
    $ hadoop jar target/GeoEnrichment-1.0-SNAPSHOT-job.jar\
     -Dcom.esri.lonField=1\
     -Dcom.esri.latField=2\
     -Dcom.esri.column="attr:POP2005:%.0f:long"\
     -Dcom.esri.searchClass=com.esri.SearchShapefileIndexPolygon\
     -Dcom.esri.writeAll=false\
     /user/cloudera/data.tsv\
     /user/cloudera/output\
     /user/cloudera/cntry06.zip

Remove the HDFS output folder. Run the Hadoop job where the longitude values are in the second (zero based) column in
the input path and the latitude values are in the third column.  By default, the fields are tab separated.
Geo enrich the output with the **POP2005** field values of type **long** from the shapefile inside **cntry06.zip**.
Represent that column as a floating point with no decimal values **%.0f**. Use the **com.esri.SearchShapefileIndexPolygon**
class to perform the GeoEnrichment using a point in polygon search.
Only write to the output path the locations that are inside the reference polygons.
And finally, the input data is located in **/user/cloudera/data.tsv**, write the job output to the folder **/user/cloudera/output**
and add **/user/cloudera/cntry06.zip** as a cached archive in the DistributedCache.

The value of *com.esri.column* is of the form:

<table>
<tr>
<td>Column Family</td><td>Qualifier</td><td>printf float format</td><td>i(nteger) or l(ong) or f(loat) or d(ouble)</td>
</tr>
</table>

## View The Output

    $ echo -e "ID\tLON\tLAT\tPOP2005" > /tmp/output.txt
    $ hadoop fs -cat /user/cloudera/output/part-* >> /tmp/output.txt

In our sample case, You can use [ArcGIS for Desktop](http://www.esri.com/software/arcgis/arcgis-for-desktop) to view results.

## Hadoop Job Description
This is a mapper only job that relies on the speed of HDFS and the distributed nature of Hadoop to plow through the logic.
Once the input is decoded into a latitude and longitude values, the GeoEnrichment search is invoked.
I wanted to try out different search environments where the GeoEnrichment layer can be in-proc or out-of-proc.
If the reference layer is small enough (ie. US Census Block Groups feature class is about 240,000 features) then an in-proc solution
makes sense as, in theory, your Hadoop data nodes should have enough memory.

### SearchShapeFileIndexPolygon
This PiP implementation relies on [GeoTools](http://geotools.org) and [JTS](http://www.vividsolutions.com/jts/JTSHome.htm) to read a
polygon shapefile from the [DistributedCache](http://hadoop.apache.org/docs/stable/mapred_tutorial.html#DistributedCache) and
create an in-memory spatial index based on the envelope of the features.
When the search function is invoked, a query is performed on that spatial index to find the overlapping feature envelopes and is
followed with a 'contains' operation to insure that it is truly in feature geometry.

**Note: for this to work very very quickly, all geometries have to be 'prepared'**

    final PreparedGeometry preparedGeometry = PreparedGeometryFactory.prepare(geometry);

### SearchShapefilePoint
This NNW search implementation relies on points rather than polygons. Again, an in-memory spatial index is created and all the
points in the GeoEnrichment layer.

### SearchHBase
There will be cases when the reference data is too big to fit into the mapper memory and has to be searched "externally".
Based on my [post on spatial index of BigData](http://thunderheadxpler.blogspot.com/2013/08/bigdata-spatial-indexes-and-hbase.html),
I will use [HBase](http://hbase.apache.org) and [GeoHashing](http://en.wikipedia.org/wiki/Geohash) to spatially perform an NNW search
The design of the rowkey in an HBase table is very important step depending on the table usage. In this case, it is heavily reliant
on a scan search of a bounded range. The reference points are placed in HBase and the rowkey is as follows:

<table><tr><td>Geohash Code</td><td>Longitude</td><td>Latitude</td><td>Unique ID</td></tr></table>

The geohash code ensures that all the reference points are "close" together for a scan, the embedding of the lat/lon values
makes the rowkey a bit more "unique" as depending on the geohash level, two "close" lat/lon coordinates might produce the same geohash.
In addition, it has the benefit of _not_ storing the coordinate in a family/qualifier value. And finally to make the rowkey really unique,
an identifier as appended - think here of a bunch of customers with unique identifiers living in the same tall building at different floors.

### SearchQuadTree
This is an InMemory PiP search where reference polygons are stored in an HBase table in the [Esri shape binary format](http://en.wikipedia.org/wiki/Shapefile).
All the polygons are retrieved into the mapper memory space and spatially indexed using the [Esri Geometry For Java](https://github.com/Esri/geometry-api-java) QuadTree implementation.
The search uses the quad tree to locate the overlapping polygons based on their envelope and then a further 'contains' operation
is performed to find out which one polygon's attributes should be used in the GeoEnrichment process.

## Test Cases
Take a look at the **src/test/java** folder for MapReduce map function testing examples and more importantly how to start
a local mini HBase cluster for scan testing and local MapReduce cluster for job testing.

## TODO
* Redo SearchShapefilePoint
* Redo SeachHBase with "correct" buffer and distance
* GeoEnrich with text attributes. In the case of weighted average, return the weight of each text attribute in descending order {text1:w1,text2:w2,...}
* Read/Write from/to Amazon S3.
* Invoke the job from Amazon EMR.
* Use InMemory database (Terracotta, Hazelcast, GridGain)

### Neat Resources
* http://www.codecogs.com/latex/htmlequations.php
* http://www.url-encode-decode.com/urlencode
