package com.esri;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MapperTest
{
    @Test
    public void testBadLine() throws Exception
    {
        final MapperDriver mapperDriver = MapperDriver.newMapperDriver(new GeoEnrichmentMapper());
        mapperDriver.
                configure(GeoEnrichmentJob.KEY_COLUMN, "fam:qual:%.1f").
                withInput(new LongWritable(0), new Text("ID\tFOO\tBAR")).
                runTest();
        Assert.assertEquals(1, mapperDriver.getCounters().findCounter(GeoEnrichmentMapper.class.getSimpleName(), GeoEnrichmentMapper.BAD_LINE).getValue());
    }

    @Test
    public void testSearchAverage05() throws Exception
    {
        final MapperDriver mapperDriver = MapperDriver.newMapperDriver(new GeoEnrichmentMapper());
        mapperDriver.
                configure(GeoEnrichmentJob.KEY_COLUMN, "fam:qual:%.1f:d").
                configure(GeoEnrichmentJob.KEY_SEARCH_CLASS, SearchAverage.class, SearchInterface.class).
                configure(GeoEnrichmentJob.KEY_BUFFER, 0.5F).
                withInput(new LongWritable(0), new Text("ID\t0.0\t0.0")).
                withOutput(NullWritable.get(), new Text("ID\t0.0\t0.0\t0.0")).
                runTest();
    }

    @Test
    public void testSearchAverage20() throws Exception
    {
        final MapperDriver mapperDriver = MapperDriver.newMapperDriver(new GeoEnrichmentMapper());
        mapperDriver.
                configure(GeoEnrichmentJob.KEY_COLUMN, "fam:qual:%.1f").
                configure(GeoEnrichmentJob.KEY_SEARCH_CLASS, SearchAverage.class, SearchInterface.class).
                configure(GeoEnrichmentJob.KEY_BUFFER, 2.0F).
                withInput(new LongWritable(0), new Text("ID\t0.0\t0.0")).
                withOutput(NullWritable.get(), new Text("ID\t0.0\t0.0\t1.0")).
                runTest();
    }

    @Test
    public void testSearchAverageWeighted() throws Exception
    {
        double count = 0.0;
        final float offset = 2.0F;
        final double ox = 0.5;
        final double oy = 0.5;
        final double w0 = calcWeight(-1, -1, ox, oy, offset);
        if (w0 > 0.0)
        {
            count++;
        }
        final double w1 = calcWeight(1, -1, ox, oy, offset);
        if (w1 > 0.0)
        {
            count++;
        }
        final double w2 = calcWeight(1, 1, ox, oy, offset);
        if (w2 > 0.0)
        {
            count++;
        }
        final double w3 = calcWeight(-1, 1, ox, oy, offset);
        if (w3 > 0.0)
        {
            count++;
        }
        final double ww = count == 0.0 ? 0.0 : (w0 + w1 + w2 + w3) / count;

        final MapperDriver mapperDriver = MapperDriver.newMapperDriver(new GeoEnrichmentMapper());
        mapperDriver.
                configure(GeoEnrichmentJob.KEY_COLUMN, "fam:qual:%.1f:d").
                configure(GeoEnrichmentJob.KEY_SEARCH_CLASS, SearchAverageWeighted.class, SearchInterface.class).
                configure(GeoEnrichmentJob.KEY_BUFFER, offset).
                withInput(new LongWritable(0), new Text(String.format("ID\t%.1f\t%.1f", ox, oy))).
                withOutput(NullWritable.get(), new Text(String.format("ID\t%.1f\t%.1f\t%.1f", ox, oy, ww))).
                runTest();
    }

    private double calcWeight(
            final double px,
            final double py,
            final double ox,
            final double oy,
            final double buffer
    )
    {
        final double dx = px - ox;
        final double dy = py - oy;
        return 1.0 - Math.min(buffer, Math.sqrt(dx * dx + dy * dy)) / buffer;
    }

}
