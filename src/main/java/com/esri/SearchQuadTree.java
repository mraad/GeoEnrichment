package com.esri;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.SpatialReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create an Esri geom QuadTree from an HBase scan
 */
public class SearchQuadTree extends SearchAbstract
{
    private final class QuadFeature
    {
        Geometry geometry;
        Map<byte[], Double> attributes;
    }

    public static final String KEY_GEOMETRY_TYPE = "com.esri.geometryType";
    public static final byte[] GEOM = "geom".getBytes();
    public static final byte[] SHAPE = "shape".getBytes();

    private Point m_point;
    private QuadTree m_quadTree;
    private List<QuadFeature> m_featureList;
    private QuadTree.QuadTreeIterator m_quadTreeIterator;
    private SpatialReference m_spatialReference;

    @Override
    public void setup(
            final Configuration configuration,
            final List<ColumnInterface> columnList) throws IOException
    {
        m_point = new Point();
        m_spatialReference = SpatialReference.create(configuration.getInt("com.esri.wkid", 4326));

        final Geometry.Type geometryType = configuration.getEnum(KEY_GEOMETRY_TYPE, Geometry.Type.Polygon);

        int index = 0;
        m_featureList = new ArrayList<QuadFeature>();
        final int height = configuration.getInt("com.esri.quadTreeHeight", 8);
        m_quadTree = new QuadTree(new Envelope2D(-180, -90, 180, 90), height);
        final HTable table = new HTable(configuration, configuration.get(GeoEnrichmentJob.KEY_TABLE));
        try
        {
            final Scan scan = new Scan();
            scan.setMaxVersions(1);
            scan.setCaching(configuration.getInt(SearchHBase.KEY_SCAN_CACHING, 128));
            scan.addColumn(GEOM, SHAPE);
            for (final ColumnInterface column : columnList)
            {
                scan.addColumn(column.getFamilyAsBytes(), column.getQualifierAsBytes());
            }
            final ResultScanner scanner = table.getScanner(scan);
            try
            {
                for (final Result result : scanner)
                {
                    final byte[] bytes = result.getValue(GEOM, SHAPE);
                    final Geometry geometry = GeometryEngine.geometryFromEsriShape(bytes, geometryType);
                    final Envelope2D envelope2D = new Envelope2D();
                    geometry.queryEnvelope2D(envelope2D);

                    final QuadFeature quadFeature = new QuadFeature();
                    quadFeature.geometry = geometry;
                    quadFeature.attributes = new HashMap<byte[], Double>();
                    for (final ColumnInterface column : columnList)
                    {
                        final byte[] value = result.getValue(column.getFamilyAsBytes(), column.getQualifierAsBytes());
                        quadFeature.attributes.put(column.getQualifierAsBytes(), column.toDouble(value));
                    }
                    m_featureList.add(quadFeature);
                    m_quadTree.insert(index++, envelope2D);
                }
            }
            finally
            {
                scanner.close();
            }
        }
        finally
        {
            table.close();
        }
        m_quadTreeIterator = m_quadTree.getIterator();
    }

    @Override
    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList) throws IOException
    {
        resetColumns(columnList);

        m_point.setX(lon);
        m_point.setY(lat);
        m_quadTreeIterator.resetIterator(m_point, 0);

        int elementIndex = m_quadTreeIterator.next();

        while (elementIndex >= 0)
        {
            final int featureIndex = m_quadTree.getElement(elementIndex);
            final QuadFeature feature = m_featureList.get(featureIndex);
            if (GeometryEngine.contains(feature.geometry, m_point, m_spatialReference))
            {
                for (final ColumnInterface column : columnList)
                {
                    column.setWeight(feature.attributes.get(column.getQualifierAsBytes()));
                }
                m_found = true;
                break;
            }
            elementIndex = m_quadTreeIterator.next();
        }
        return m_found;
    }
}
