package com.esri;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 */
public abstract class SearchAbstract implements SearchInterface
{
    protected boolean m_found;

    protected void resetColumns(final List<ColumnInterface> columnList)
    {
        m_found = false;
        for (final ColumnInterface column : columnList)
        {
            column.reset();
        }
    }

    @Override
    public void cleanup(final Configuration configuration) throws IOException
    {
    }
}
