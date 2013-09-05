hadoop fs -rm -R -skipTrash output
hadoop jar target/GeoEnrichment-1.0-SNAPSHOT-job.jar\
 -Dcom.esri.lonField=1\
 -Dcom.esri.latField=2\
 -Dcom.esri.columns="attr|POP2005|%.0f|l"\
 -Dcom.esri.searchClass=com.esri.SearchShapefileIndexPolygon\
 -Dcom.esri.writeAll=false\
 /user/cloudera/data.tsv\
 /user/cloudera/output\
 /user/cloudera/cntry06.zip
