package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 */
@Deprecated
public class SearchShapefilePointMapperTest extends SearchShapefileTesting
{

    private File m_file;

    public static class TestSearchPointShapefile extends SearchShapefilePoint
    {
        @Override
        protected URL getUrl(final Configuration configuration) throws MalformedURLException
        {
            return new URL(configuration.get("com.esri.url"));
        }
    }

    @Before
    public void setup() throws IOException
    {
        m_file = createPointShapefile();
    }

    @Test
    public void testSearchPointShapefileMapper() throws Exception
    {
        final List<Pair<LongWritable, Text>> inputList = new ArrayList<Pair<LongWritable, Text>>();
        inputList.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text("ID\t-1.0\t-1.0")));
        inputList.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text("ID\t1.0\t1.0")));

        final MapperDriver mapperDriver = MapperDriver.newMapperDriver(new GeoEnrichmentMapper());
        mapperDriver.
                configure("com.esri.url", m_file.toURI().toURL().toString()).
                configure(GeoEnrichmentJob.KEY_COLUMN, "fam:qual:%.1f").
                configure(GeoEnrichmentJob.KEY_SEARCH_CLASS, TestSearchPointShapefile.class, SearchInterface.class).
                configure(GeoEnrichmentJob.KEY_BUFFER, 0.5F).
                withAll(inputList).
                withOutput(NullWritable.get(), new Text("ID\t-1.0\t-1.0\t1.0")).
                withOutput(NullWritable.get(), new Text("ID\t1.0\t1.0\t1.0")).
                runTest(false);
    }
}
