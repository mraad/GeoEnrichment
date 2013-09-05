package com.esri;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import com.vividsolutions.jts.index.ItemVisitor;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.hadoop.conf.Configuration;
import org.geotools.data.DataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.util.NullProgressListener;
import org.opengis.feature.Feature;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.Property;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class SearchShapefileIndexPolygon extends SearchAbstract implements ItemVisitor
{
    final class PreparedFeature
    {
        Feature feature;
        PreparedGeometry preparedGeometry;

        private PreparedFeature(
                final Feature feature,
                final PreparedGeometry preparedGeometry)
        {
            this.feature = feature;
            this.preparedGeometry = preparedGeometry;
        }
    }

    private GeometryFactory m_geometryFactory = JTSFactoryFinder.getGeometryFactory();
    private Point m_point;

    private Coordinate m_coordinate = new Coordinate();
    private Envelope m_envelope = new Envelope();
    private double m_buffer;
    private SpatialIndex m_spatialIndex;
    private String m_geometryName;
    private List<ColumnInterface> m_columnList;

    @Override
    public void setup(
            final Configuration configuration,
            final List<ColumnInterface> columnList) throws IOException
    {
        m_spatialIndex = new STRtree();
        m_buffer = configuration.getFloat(GeoEnrichmentJob.KEY_BUFFER, 0.000001F);

        final URL url = getUrl(configuration);
        final ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
        final Map params = new HashMap();
        params.put(ShapefileDataStoreFactory.URLP.key, url);
        final DataStore dataStore = factory.createDataStore(params);
        try
        {
            final String[] typeNames = dataStore.getTypeNames();
            final SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeNames[0]);
            m_geometryName = featureSource.getSchema().getGeometryDescriptor().getLocalName();
            final SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            featureCollection.accepts(new FeatureVisitor()
            {
                public void visit(final Feature feature)
                {
                    final Geometry geometry = (Geometry) feature.getProperty(m_geometryName).getValue();
                    final PreparedGeometry preparedGeometry = PreparedGeometryFactory.prepare(geometry);
                    m_spatialIndex.insert(geometry.getEnvelopeInternal(), new PreparedFeature(feature, preparedGeometry));
                }
            }, new NullProgressListener());
        }
        finally
        {
            dataStore.dispose();
        }
    }

    protected URL getUrl(final Configuration configuration) throws IOException
    {
        final File file = new File("./shapefile");
        return file.toURI().toURL();
    }

    @Override
    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList) throws IOException
    {
        m_columnList = columnList;

        resetColumns(columnList);

        m_coordinate.x = lon;
        m_coordinate.y = lat;

        m_point = m_geometryFactory.createPoint(m_coordinate);

        final double xmin = m_coordinate.x - m_buffer;
        final double ymin = m_coordinate.y - m_buffer;
        final double xmax = m_coordinate.x + m_buffer;
        final double ymax = m_coordinate.y + m_buffer;
        m_envelope.init(xmin, xmax, ymin, ymax);

        m_spatialIndex.query(m_envelope, this);

        return m_found;
    }

    @Override
    public void visitItem(final Object o)
    {
        final PreparedFeature preparedFeature = (PreparedFeature) o;
        if (preparedFeature.preparedGeometry.contains(m_point))
        {
            m_found = true;
            for (final ColumnInterface column : m_columnList)
            {
                final Property property = preparedFeature.feature.getProperty(column.getQualifier());
                if (property != null)
                {
                    column.setWeight(column.toDouble(property.getValue()));
                }
            }
        }
    }
}
