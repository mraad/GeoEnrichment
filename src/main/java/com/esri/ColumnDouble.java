package com.esri;

import org.apache.hadoop.hbase.util.Bytes;

/**
 */
public class ColumnDouble extends ColumnAbstract
{
    public ColumnDouble(
            final String family,
            final String qualifier,
            final String format)
    {
        super(family, qualifier, format);
    }

    @Override
    public double toDouble(final byte[] bytes)
    {
        return Bytes.toDouble(bytes);
    }

    @Override
    public double toDouble(final Object obj)
    {
        return (Double) obj;
    }


}
