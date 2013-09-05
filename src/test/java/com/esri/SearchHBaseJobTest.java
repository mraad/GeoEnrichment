package com.esri;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 */
public class SearchHBaseJobTest extends HBaseTesting
{
    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        m_testUtil.startMiniMapReduceCluster();
    }

    @Test
    public void testSearchHBaseJob() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException
    {
        writeInput(0.5, 0.5);

        setConfigurationKeys(m_testUtil.getConfiguration(), 2.0F);

        final String[] args = {"data.tsv", "output"};
        GeoEnrichmentJob.createSubmittableJob(m_testUtil.getConfiguration(), args).waitForCompletion(true);

        readOutput(0.5, 0.5, 2.0F);
    }

    private void setConfigurationKeys(
            final Configuration configuration,
            final float buffer)
    {
        configuration.set(GeoEnrichmentJob.KEY_TABLE, new String(TAB));
        configuration.set(GeoEnrichmentJob.KEY_COLUMNS, "fam|qual|%.1f");
        configuration.setClass(GeoEnrichmentJob.KEY_SEARCH_CLASS, SearchHBase.class, SearchInterface.class);
        configuration.setFloat(GeoEnrichmentJob.KEY_BUFFER, buffer);
    }

    private void readOutput(
            final double x,
            final double y,
            final float offset) throws IOException
    {
        final FileSystem fileSystem = FileSystem.get(m_testUtil.getConfiguration());
        final Path output = new Path("output");
        Assert.assertTrue(fileSystem.exists(output));
        final Path success = new Path("output/_SUCCESS");
        Assert.assertTrue(fileSystem.exists(success));
        final Path part = new Path("output/part-m-00000");
        Assert.assertTrue(fileSystem.exists(part));
        final FSDataInputStream inputStream = fileSystem.open(part);
        try
        {
            final String content = IOUtils.toString(inputStream);
            final double w = calcExpectedWeight(x, y, offset);
            final String expected = String.format("ID\t%.1f\t%.1f\t%.1f\n", x, y, w);
            Assert.assertEquals(expected, content);
        }
        finally
        {
            IOUtils.closeQuietly(inputStream);
        }
    }

    private void writeInput(
            final double x,
            final double y) throws IOException
    {
        final FileSystem fileSystem = FileSystem.get(m_testUtil.getConfiguration());
        final FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("data.tsv"), true);
        try
        {
            fsDataOutputStream.writeBytes(String.format("ID\t%.1f\t%.1f\n", x, y));
        }
        finally
        {
            fsDataOutputStream.close();
        }
    }

    @After
    public void tearDown() throws Exception
    {
        m_testUtil.shutdownMiniMapReduceCluster();
        super.tearDown();
    }

}
