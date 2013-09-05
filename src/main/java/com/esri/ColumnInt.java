package com.esri;

import org.apache.hadoop.hbase.util.Bytes;

/**
 */
public class ColumnInt extends ColumnAbstract
{
    public ColumnInt(
            final String family,
            final String qualifier,
            final String format)
    {
        super(family, qualifier, format);
    }

    @Override
    public double toDouble(final byte[] bytes)
    {
        return Bytes.toInt(bytes);
    }

    @Override
    public double toDouble(final Object obj)
    {
        return (Integer) obj;
    }
}
