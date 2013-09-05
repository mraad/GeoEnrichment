package com.esri;

import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 */
public final class SearchNoop implements SearchInterface
{
    @Override
    public void setup(
            final Configuration configuration,
            final List<ColumnInterface> columnList)
    {
    }

    @Override
    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList)
    {
        return true;
    }

    @Override
    public void cleanup(final Configuration configuration)
    {
    }
}
