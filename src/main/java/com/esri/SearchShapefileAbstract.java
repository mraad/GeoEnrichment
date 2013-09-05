package com.esri;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.hadoop.conf.Configuration;
import org.geotools.data.DataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.util.NullProgressListener;
import org.opengis.filter.FilterFactory2;
import org.opengis.util.ProgressListener;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@Deprecated
public abstract class SearchShapefileAbstract extends SearchAbstract
{
    protected GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
    protected Coordinate m_coordinate = new Coordinate();
    protected SimpleFeatureSource m_featureSource;
    protected FilterFactory2 m_filterFactory;
    protected DataStore m_dataStore;
    protected ProgressListener m_progressListener;
    protected String m_geometryName;
    protected double m_buffer;

    @Override
    public void setup(
            final Configuration configuration,
            final List<ColumnInterface> columnList) throws IOException
    {
        final URL url = getUrl(configuration);
        final ShapefileDataStoreFactory fac = new ShapefileDataStoreFactory();
        final Map params = new HashMap();
        params.put(ShapefileDataStoreFactory.URLP.key, url);
        params.put(ShapefileDataStoreFactory.CREATE_SPATIAL_INDEX.key, Boolean.TRUE);
        params.put(ShapefileDataStoreFactory.ENABLE_SPATIAL_INDEX, Boolean.TRUE);
        params.put(ShapefileDataStoreFactory.MEMORY_MAPPED, Boolean.TRUE);
        m_dataStore = fac.createDataStore(params);

        final String[] typeNames = m_dataStore.getTypeNames();
        m_featureSource = m_dataStore.getFeatureSource(typeNames[0]);
        m_geometryName = m_featureSource.getSchema().getGeometryDescriptor().getLocalName();

        m_filterFactory = CommonFactoryFinder.getFilterFactory2(null);
        m_progressListener = new NullProgressListener();
        m_buffer = configuration.getFloat(GeoEnrichmentJob.KEY_BUFFER, 0.000001F);
    }

    protected URL getUrl(final Configuration configuration) throws IOException
    {
        final File file = new File("./shapefile");
        return file.toURI().toURL();
    }

    @Override
    public void cleanup(final Configuration configuration) throws IOException
    {
        if (m_dataStore != null)
        {
            m_dataStore.dispose();
            m_dataStore = null;
        }
    }
}
