package com.esri;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 */
public interface SearchInterface
{
    public void setup(
            final Configuration configuration,
            final List<ColumnInterface> columnList) throws IOException;

    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList) throws IOException;

    public void cleanup(final Configuration configuration) throws IOException;
}
