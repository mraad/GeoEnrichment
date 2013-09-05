package com.esri;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 */
public class QuadRowKey
{
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(128);
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    private void writeBytes(
            final double x,
            final double y) throws IOException
    {
        byteArrayOutputStream.reset();
        dataOutputStream.writeLong(Quad.encode(x, y));
        dataOutputStream.writeDouble(x);
        dataOutputStream.writeDouble(y);
    }

    public byte[] toBytes(
            final double x,
            final double y) throws IOException
    {
        writeBytes(x, y);
        return byteArrayOutputStream.toByteArray();
    }

    public byte[] toBytes(
            final double x,
            final double y,
            final long uuid) throws IOException
    {
        writeBytes(x, y);
        dataOutputStream.writeLong(uuid);
        return byteArrayOutputStream.toByteArray();
    }

}
