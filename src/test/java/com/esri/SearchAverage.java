package com.esri;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class SearchAverage extends SearchAbstract
{
    private List<Feature> m_featureList;

    protected float m_buffer;

    @Override
    public void setup(
            final Configuration configuration,
            final List<ColumnInterface> columnList) throws IOException
    {
        m_buffer = configuration.getFloat(GeoEnrichmentJob.KEY_BUFFER, 0.5F);
        m_featureList = new ArrayList<Feature>();

        m_featureList.add(createFeature(1, 1, 1));
        m_featureList.add(createFeature(-1, 1, 1));
        m_featureList.add(createFeature(1, -1, 1));
        m_featureList.add(createFeature(-1, -1, 1));
    }

    private Feature createFeature(
            final double lon,
            final double lat,
            final double value)
    {
        final Feature feature = new Feature();
        feature.lon = lon;
        feature.lat = lat;
        feature.val = value;
        return feature;
    }

    @Override
    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList) throws IOException
    {
        resetColumns(columnList);

        final double lonmin = lon - m_buffer;
        final double lonmax = lon + m_buffer;
        final double latmin = lat - m_buffer;
        final double latmax = lat + m_buffer;
        for (final Feature feature : m_featureList)
        {
            if (feature.lon < lonmin)
            {
                continue;
            }
            if (feature.lon > lonmax)
            {
                continue;
            }
            if (feature.lat < latmin)
            {
                continue;
            }
            if (feature.lat > latmax)
            {
                continue;
            }
            final double weight = calcWeight(feature, lon, lat);
            if (weight > 0.0)
            {
                m_found = true;
                for (final ColumnInterface column : columnList)
                {
                    column.addWeight(weight * feature.val);
                }
            }
        }
        return m_found;
    }

    protected double calcWeight(
            final Feature feature,
            final double lon,
            final double lat)
    {
        return 1.0;
    }
}
