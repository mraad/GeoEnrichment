package com.esri;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
public class QuadRowKey
{
    final ByteBuffer m_byteBuffer = ByteBuffer.allocate(128);

    private void writeBytes(
            final double x,
            final double y) throws IOException
    {
        m_byteBuffer.clear();
        m_byteBuffer.putLong(Quad.encode(x, y));
        m_byteBuffer.putDouble(x);
        m_byteBuffer.putDouble(y);
    }

    public byte[] toBytes(
            final double x,
            final double y) throws IOException
    {
        writeBytes(x, y);
        return m_byteBuffer.array();
    }

    public byte[] toBytes(
            final double x,
            final double y,
            final long uuid) throws IOException
    {
        writeBytes(x, y);
        m_byteBuffer.putLong(uuid);
        return m_byteBuffer.array();
    }

}
