package com.esri;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 */
public class HBaseTesting
{
    protected final HBaseTestingUtility m_testUtil = new HBaseTestingUtility();
    protected final byte[] TAB = "lut".getBytes();
    protected final byte[] FAM = "fam".getBytes();
    protected final byte[] QUAL = "qual".getBytes();

    @Before
    public void setUp() throws Exception
    {
        m_testUtil.startMiniCluster();
        createTable();
    }

    private void createTable() throws IOException
    {
        final HTable table = m_testUtil.createTable(TAB, FAM);
        try
        {
            final QuadRowKey quadRowKey = new QuadRowKey();

            putInTable(table, quadRowKey, 1.0, 1.0, 1.0);
            putInTable(table, quadRowKey, -1.0, 1.0, 1.0);
            putInTable(table, quadRowKey, 1.0, -1.0, 1.0);
            putInTable(table, quadRowKey, -1.0, -1.0, 1.0);
        }
        finally
        {
            table.close();
        }
    }

    private void putInTable(
            final HTable table,
            final QuadRowKey quadRowKey,
            final double x,
            final double y,
            final double w) throws IOException
    {
        final Put put = new Put(quadRowKey.toBytes(x, y));
        put.add(FAM, QUAL, Bytes.toBytes(w));
        table.put(put);
    }

    protected double calcWeight(
            final double px,
            final double py,
            final double ox,
            final double oy,
            final double ofs
    )
    {
        final double dx = px - ox;
        final double dy = py - oy;
        final double dd = Math.sqrt(dx * dx + dy * dy);
        return 1.0 - Math.min(ofs, dd) / ofs;
    }

    protected double calcExpectedWeight(
            final double ox,
            final double oy,
            final float offset)
    {
        double count = 0.0;
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
        return count == 0.0 ? 0.0 : (w0 + w1 + w2 + w3) / count;
    }

    @After
    public void tearDown() throws Exception
    {
        m_testUtil.deleteTable(TAB);
        m_testUtil.shutdownMiniCluster();
    }
}
