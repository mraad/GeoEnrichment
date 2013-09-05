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
public class SearchShapefilePolygon extends SearchShapefileAbstract
{
    @Override
    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList) throws IOException
    {
        resetColumns(columnList);

        m_coordinate.x = lon;
        m_coordinate.y = lat;

        final double xmin = m_coordinate.x - m_buffer;
        final double ymin = m_coordinate.y - m_buffer;
        final double xmax = m_coordinate.x + m_buffer;
        final double ymax = m_coordinate.y + m_buffer;
        final Point point = geometryFactory.createPoint(m_coordinate);

        final BBOX bbox = m_filterFactory.bbox(m_geometryName, xmin, ymin, xmax, ymax, null);

        final SimpleFeatureCollection featureCollection = m_featureSource.getFeatures(bbox);
        featureCollection.accepts(new FeatureVisitor()
        {
            public void visit(final Feature feature)
            {
                final Geometry geometry = (Geometry) feature.getProperty(m_geometryName).getValue();
                if (point.within(geometry))
                {
                    m_found = true;
                    for (final ColumnInterface column : columnList)
                    {
                        final Property property = feature.getProperty(column.getQualifier());
                        if (property != null)
                        {
                            column.setWeight(column.toDouble(property.getValue()));
                        }
                    }
                }
            }
        }, m_progressListener);

        return m_found;
    }
}
