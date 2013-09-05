package com.esri;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class SearchShapefileTesting
{
    final private GeometryFactory m_geometryFactory = JTSFactoryFinder.getGeometryFactory();

    protected File m_file;

    protected File createPolygonShapefile() throws IOException
    {
        final SimpleFeatureType featureType = getSimpleFeatureType(Polygon.class);

        final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);

        final DefaultFeatureCollection featureCollection = new DefaultFeatureCollection(null, featureType);

        final Coordinate[] coordinates = new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(10, 0),
                new Coordinate(10, 10),
                new Coordinate(5, 5),
                new Coordinate(0, 10),
                new Coordinate(0, 0)
        };

        featureBuilder.reset();
        featureBuilder.add(m_geometryFactory.createPolygon(coordinates));
        featureBuilder.add(1234.5);
        featureCollection.add(featureBuilder.buildFeature("FID"));

        return getFile(featureType, featureCollection);
    }

    protected File createPointShapefile() throws IOException
    {
        final SimpleFeatureType featureType = getSimpleFeatureType(Point.class);

        final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);

        final DefaultFeatureCollection featureCollection = new DefaultFeatureCollection(null, featureType);

        featureBuilder.reset();
        featureBuilder.add(m_geometryFactory.createPoint(new Coordinate(-1.0, -1.0)));
        featureBuilder.add(1.0);
        featureCollection.add(featureBuilder.buildFeature("FID-1"));

        featureBuilder.reset();
        featureBuilder.add(m_geometryFactory.createPoint(new Coordinate(1.0, 1.0)));
        featureBuilder.add(1.0);
        featureCollection.add(featureBuilder.buildFeature("FID-2"));

        return getFile(featureType, featureCollection);
    }

    private SimpleFeatureType getSimpleFeatureType(final Class<?> aClass)
    {
        final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("shapefile");
        builder.setCRS(DefaultGeographicCRS.WGS84);
        builder.add("the_geom", aClass);
        builder.add("qual", Double.class);
        return builder.buildFeatureType();
    }

    private File getFile(
            final SimpleFeatureType featureType,
            final DefaultFeatureCollection collection) throws IOException
    {
        final ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

        final File file = File.createTempFile("shapefile-", ".shp");
        file.deleteOnExit();

        final Map<String, Serializable> params = new HashMap<String, Serializable>();
        params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());

        final ShapefileDataStore dataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
        try
        {
            dataStore.createSchema(featureType);

            final String typeName = dataStore.getTypeNames()[0];
            final SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
            if (featureSource instanceof SimpleFeatureStore)
            {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
                final Transaction transaction = new DefaultTransaction("create");
                try
                {
                    featureStore.setTransaction(transaction);
                    featureStore.addFeatures(collection);
                    transaction.commit();
                }
                catch (Exception e)
                {
                    transaction.rollback();
                }
                finally
                {
                    transaction.close();
                }
            }
        }
        finally
        {
            dataStore.dispose();
        }
        return file;
    }
}
