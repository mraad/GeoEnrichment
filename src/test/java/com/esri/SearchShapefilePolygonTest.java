package com.esri;

import org.apache.hadoop.conf.Configuration;

import java.net.MalformedURLException;
import java.net.URL;

/**
 */
@Deprecated
public class SearchShapefilePolygonTest extends SearchShapefilePolygonTesting
{
    private class TestSearchPolygonShapefile extends SearchShapefilePolygon
    {
        @Override
        protected URL getUrl(final Configuration configuration) throws MalformedURLException
        {
            return m_file.toURI().toURL();
        }
    }

    @Override
    protected SearchInterface createSearchInterfaceImplementation()
    {
        return new TestSearchPolygonShapefile();
    }

}
