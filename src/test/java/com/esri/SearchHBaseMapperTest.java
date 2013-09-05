package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 */
public class SearchHBaseMapperTest extends HBaseTesting
{
    private void setConfiguration(final MapperDriver mapperDriver)
    {
        final Configuration configuration = mapperDriver.getConfiguration();
        for (final Map.Entry<String, String> entry : m_testUtil.getConfiguration())
        {
            configuration.set(entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testSearchHBaseMapper() throws Exception
    {
        final double ox = 0.5;
        final double oy = 0.5;
        final float offset = 2.0F;

        final double ww = calcExpectedWeight(ox, oy, offset);

        final MapperDriver mapperDriver = MapperDriver.newMapperDriver(new GeoEnrichmentMapper());
        setConfiguration(mapperDriver);
        mapperDriver.
                configure(GeoEnrichmentJob.KEY_TABLE, new String(TAB)).
                configure(GeoEnrichmentJob.KEY_COLUMNS, "fam|qual|%.1f").
                configure(GeoEnrichmentJob.KEY_SEARCH_CLASS, SearchHBase.class, SearchInterface.class).
                configure(GeoEnrichmentJob.KEY_BUFFER, offset).
                withInput(new LongWritable(0), new Text(String.format("ID\t%.1f\t%.1f", ox, oy))).
                withOutput(NullWritable.get(), new Text(String.format("ID\t%.1f\t%.1f\t%.1f", ox, oy, ww))).
                runTest();

        Assert.assertEquals(0, mapperDriver.getCounters().findCounter(GeoEnrichmentMapper.class.getSimpleName(), GeoEnrichmentMapper.BAD_LINE).getValue());
    }

}
