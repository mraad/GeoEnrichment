package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 */
@Deprecated
public class SearchShapefilePolygonMapperTest extends SearchShapefileTesting
{
    public static class TestSearchPolygonShapefile extends SearchShapefilePolygon
    {
        @Override
        protected URL getUrl(final Configuration configuration) throws MalformedURLException
        {
            return new URL(configuration.get("com.esri.url"));
        }
    }

    @Test
    public void testSearchShapefileMapper() throws Exception
    {
        final File file = createPolygonShapefile();

        final List<Pair<LongWritable, Text>> inputList = new ArrayList<Pair<LongWritable, Text>>();
        inputList.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text("ID\t2.5\t2.5")));
        inputList.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text("ID\t5.0\t8.0")));

        final MapperDriver mapperDriver = MapperDriver.newMapperDriver(new GeoEnrichmentMapper());
        mapperDriver.
                configure("com.esri.url", file.toURI().toURL().toString()).
                configure(GeoEnrichmentJob.KEY_COLUMN, "fam:qual:%.1f").
                configure(GeoEnrichmentJob.KEY_SEARCH_CLASS, TestSearchPolygonShapefile.class, SearchInterface.class).
                configure(GeoEnrichmentJob.KEY_BUFFER, 0.000001F).
                withAll(inputList).
                withOutput(NullWritable.get(), new Text("ID\t2.5\t2.5\t1234.5")).
                withOutput(NullWritable.get(), new Text("ID\t5.0\t8.0\t0.0")).
                runTest(false);
    }
}
