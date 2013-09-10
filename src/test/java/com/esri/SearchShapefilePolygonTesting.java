package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 */
public abstract class SearchShapefilePolygonTesting extends SearchShapefileTesting
{
    private List<ColumnInterface> m_columnlist;
    private SearchInterface m_search;
    private Configuration m_configuration;

    protected abstract SearchInterface createSearchInterfaceImplementation();

    @Before
    public void setUp() throws Exception
    {
        m_configuration = new Configuration();

        m_file = createPolygonShapefile();

        m_columnlist = ColumnParser.newInstance().parseColumn("geom:qual:%.1f");
        m_search = createSearchInterfaceImplementation();
        m_search.setup(m_configuration, m_columnlist);
    }

    @Test
    public void testSearchPolygonShapefile() throws Exception
    {
        m_search.search(2.5, 2.5, m_columnlist);
        Assert.assertEquals(1234.5, m_columnlist.get(0).getValue(), 0.000001);
        m_search.search(5.0, 8.0, m_columnlist);
        Assert.assertEquals(0.0, m_columnlist.get(0).getValue(), 0.000001);
    }

    @After
    public void tearDown() throws Exception
    {
        m_search.cleanup(m_configuration);
    }

}
