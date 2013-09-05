package com.esri;

import org.apache.hadoop.conf.Configuration;

import java.net.MalformedURLException;
import java.net.URL;

/**
 */
public class SearchShapefileIndexPolygonTest extends SearchShapefilePolygonTesting
{
    private class TestSearchShapefileIndexPolygon extends SearchShapefileIndexPolygon
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
        return new TestSearchShapefileIndexPolygon();
    }
}
