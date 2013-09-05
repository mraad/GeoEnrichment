GeoEnrichment
=============

Hadoop MapReduce job to perform GeoEnrichment on BigData.

What is GeoEnrichment? Simply put; given a big set of locations, I would like to invoke this job to output the original
data with additional attributes derived from geo spatial information. For example, given a big set of customer locations,
I would like each location to be geo enriched with the average income of the zip code where that location falls and the
number of people between the age of 25 and 30.

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

Of course the key to this whole thing is the data !!!

### Dependencies

    $ git clone https://github.com/kungfoo/geohash-java.git
    $ mvn install

    $ git clone https://github.com/Esri/geometry-api-java.git
    $ mvn install

## Build and package

**Note: On my MacBook Pro, I had to symbolic soft link java to /bin/java to pass the maven test phase:**

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
     -Dcom.esri.columns="attr|POP2005|%.0f|long"\
     -Dcom.esri.searchClass=com.esri.SearchShapefileIndexPolygon\
     -Dcom.esri.writeAll=false\
     /user/cloudera/data.tsv\
     /user/cloudera/output\
     /user/cloudera/cntry06.zip

Remove the HDFS output folder. Run the Hadoop job where the longitude values are in the second (zero based) column in
the input path and the latitude values are in the third column.  By default, the fields are tab separated.
Geo enrich the output with the _POP2005_ field values of type _long_ from the shapefile in _cntry06.zip_.
Represent that column as a floating point with no decimal values (%.0f). Use the _com.esri.SearchShapefileIndexPolygon_ class to
perform the GeoEnrichment point in polygon search.
Only write to the output path the locations that are inside the country polygons.
Finally, the input path is _/user/cloudera/data.tsv_, the output path is _/user/cloudera/output_ and add _/user/cloudera/cntry06.zip_
as a cached archive in the DistributedCache.

## View The Output

    $ echo -e "ID\tLON\tLAT\tPOP2005" > /tmp/output.txt
    $ hadoop fs -cat /user/cloudera/output/part-* >> /tmp/output.txt
