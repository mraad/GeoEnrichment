package com.esri;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.Feature;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.Property;
import org.opengis.filter.spatial.BBOX;

import java.io.IOException;
import java.util.List;

/**
 */
@Deprecated
public class SearchShapefilePoint extends SearchShapefileAbstract
{
    public SearchShapefilePoint()
    {
    }

    @Override
    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList) throws IOException
    {
        resetColumns(columnList);

        m_coordinate.x = lon;
        m_coordinate.y = lat;

        final Point point = geometryFactory.createPoint(m_coordinate);

        // final Geometry buffer = point.buffer(m_buffer);

        final BBOX bbox = m_filterFactory.bbox(m_geometryName,
                lon - m_buffer,
                lat - m_buffer,
                lon + m_buffer,
                lat + m_buffer, null);

        final SimpleFeatureCollection featureCollection = m_featureSource.getFeatures(bbox);
        featureCollection.accepts(new FeatureVisitor()
        {
            public void visit(final Feature feature)
            {
                final Geometry geometry = (Geometry) feature.getProperty(m_geometryName).getValue();
                final double distance = geometry.distance(point);
                final double weight = 1.0 - Math.min(m_buffer, distance) / m_buffer;
                if (weight > 0.0)
                {
                    m_found = true;
                    for (final ColumnInterface column : columnList)
                    {
                        final Property property = feature.getProperty(column.getQualifier());
                        column.addWeight(weight * column.toDouble(property.getValue()));
                    }
                }
            }
        }, m_progressListener);

        return m_found;
    }

}
