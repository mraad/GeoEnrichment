package com.esri;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 */
public class SearchQuadTreeTest
{
    protected final HBaseTestingUtility m_testUtil = new HBaseTestingUtility();

    private final byte[] TAB = "lut".getBytes();
    private final byte[] QUAL = "qual".getBytes();

    private List<ColumnInterface> m_columnlist;
    private SearchQuadTree m_searchQuadTree;

    @Before
    public void setUp() throws Exception
    {
        m_testUtil.startMiniCluster();
        m_columnlist = ColumnParser.newInstance().parseColumn("geom|qual|%.1f");
        createTable();
        m_searchQuadTree = new SearchQuadTree();
        m_testUtil.getConfiguration().set(GeoEnrichmentJob.KEY_TABLE, new String(TAB));
        m_testUtil.getConfiguration().setEnum(SearchQuadTree.KEY_GEOMETRY_TYPE, Geometry.Type.Envelope);
        m_searchQuadTree.setup(m_testUtil.getConfiguration(), m_columnlist);
    }

    private void createTable() throws IOException
    {
        final HTable table = m_testUtil.createTable(TAB, SearchQuadTree.GEOM);
        try
        {
            putInTable(table, 0, new Envelope(0, 0, 10, 10), 10);
            putInTable(table, 1, new Envelope(-10, -10, 0, 0), -10);
        }
        finally
        {
            table.close();
        }
    }

    private void putInTable(
            final HTable table,
            final int id,
            final Geometry geometry,
            final double value) throws IOException
    {
        final Put put = new Put(Bytes.toBytes(id));
        put.add(SearchQuadTree.GEOM, SearchQuadTree.SHAPE, GeometryEngine.geometryToEsriShape(geometry));
        put.add(SearchQuadTree.GEOM, QUAL, Bytes.toBytes(value));
        table.put(put);
    }

    @Test
    public void testSearchQuadTree() throws IOException
    {
        m_searchQuadTree.search(5, 5, m_columnlist);
        Assert.assertEquals(10.0, m_columnlist.get(0).getValue(), 0.000001);
        m_searchQuadTree.search(-5, -5, m_columnlist);
        Assert.assertEquals(-10.0, m_columnlist.get(0).getValue(), 0.000001);
        m_searchQuadTree.search(5, -5, m_columnlist);
        Assert.assertEquals(0.0, m_columnlist.get(0).getValue(), 0.000001);
    }

    @After
    public void tearDown() throws Exception
    {
        m_searchQuadTree.cleanup(m_testUtil.getConfiguration());
        m_testUtil.deleteTable(TAB);
        m_testUtil.shutdownMiniCluster();
    }
}