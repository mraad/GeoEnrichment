package com.esri;

/**
 */
public class SearchAverageWeighted extends SearchAverage
{
    @Override
    protected double calcWeight(
            final Feature feature,
            final double lon,
            final double lat)
    {
        final double dlon = feature.lon - lon;
        final double dlat = feature.lat - lat;
        final double dist = Math.min(m_buffer, Math.sqrt(dlon * dlon + dlat * dlat));
        return 1.0 - dist / m_buffer;
    }
}
