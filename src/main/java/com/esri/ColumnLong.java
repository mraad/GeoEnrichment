package com.esri;

import org.apache.hadoop.hbase.util.Bytes;

/**
 */
public class ColumnLong extends ColumnAbstract
{
    public ColumnLong(
            final String family,
            final String qualifier,
            final String format)
    {
        super(family, qualifier, format);
    }

    @Override
    public double toDouble(final byte[] bytes)
    {
        return Bytes.toLong(bytes);
    }

    @Override
    public double toDouble(final Object obj)
    {
        return (Long) obj;
    }
}
