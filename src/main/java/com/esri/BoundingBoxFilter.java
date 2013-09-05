package com.esri;

import ch.hsr.geohash.BoundingBox;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
 * HBase filter used during scan to exclude record with points not in supplied bbox
 */
public final class BoundingBoxFilter extends FilterBase
{
    private BoundingBox m_boundingBox;

    public BoundingBoxFilter()
    {
    }

    public BoundingBoxFilter(
            final BoundingBox boundingBox
    )
    {
        m_boundingBox = boundingBox;
    }

    public BoundingBox getBoundingBox()
    {
        return m_boundingBox;
    }

    @Override
    public boolean filterRowKey(
            final byte[] bytes,
            final int offset,
            final int length)
    {
        boolean filter;
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes, offset, length);
        final DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        try
        {
            final long bits = dataInputStream.readLong();
            final double lon = dataInputStream.readDouble();
            final double lat = dataInputStream.readDouble();
            if (lon < m_boundingBox.getMinLon())
            {
                filter = true;
            }
            else if (lon > m_boundingBox.getMaxLon())
            {
                filter = true;
            }
            else if (lat < m_boundingBox.getMinLat())
            {
                filter = true;
            }
            else if (lat > m_boundingBox.getMaxLat())
            {
                filter = true;
            }
            else
            {
                filter = false;
            }
        }
        catch (IOException e)
        {
            filter = true;
        }
        return filter;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(m_boundingBox.getMinLat());
        dataOutput.writeDouble(m_boundingBox.getMaxLat());
        dataOutput.writeDouble(m_boundingBox.getMinLon());
        dataOutput.writeDouble(m_boundingBox.getMaxLon());
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {

        final double ymin = dataInput.readDouble();
        final double ymax = dataInput.readDouble();
        final double xmin = dataInput.readDouble();
        final double xmax = dataInput.readDouble();
        m_boundingBox = new BoundingBox(ymin, ymax, xmin, xmax);
    }
}
