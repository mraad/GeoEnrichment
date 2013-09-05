package com.esri;

import org.apache.hadoop.hbase.util.Bytes;

/**
 */
public class ColumnFloat extends ColumnAbstract
{
    public ColumnFloat(
            final String family,
            final String qualifier,
            final String format)
    {
        super(family, qualifier, format);
    }

    @Override
    public double toDouble(final byte[] bytes)
    {
        return Bytes.toFloat(bytes);
    }

    @Override
    public double toDouble(final Object obj)
    {
        return (Float) obj;
    }
}
