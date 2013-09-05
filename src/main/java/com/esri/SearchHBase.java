package com.esri;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.queries.GeoHashBoundingBoxQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 */
public class SearchHBase extends SearchAbstract
{
    public static final String KEY_SCAN_CACHING = "com.esri.scanCaching";

    private float m_buffer;
    private int m_scanCaching;
    private HTable m_table;

    @Override
    public void setup(
            final Configuration configuration,
            final List<ColumnInterface> columnList) throws IOException
    {
        m_table = new HTable(configuration, configuration.get(GeoEnrichmentJob.KEY_TABLE));
        m_buffer = configuration.getFloat(GeoEnrichmentJob.KEY_BUFFER, 0.5F);
        m_scanCaching = configuration.getInt(KEY_SCAN_CACHING, 50);
    }

    @Override
    public boolean search(
            final double lon,
            final double lat,
            final List<ColumnInterface> columnList) throws IOException
    {
        resetColumns(columnList);

        // Dummy implementation - should use buffer and then get envelope
        final BoundingBox boundingBox = new BoundingBox(
                Math.max(-90, lat - m_buffer),
                Math.min(90, lat + m_buffer),
                Math.max(-180, lon - m_buffer),
                Math.min(180, lon + m_buffer));

        final GeoHashBoundingBoxQuery geoHashBoundingBoxQuery = new GeoHashBoundingBoxQuery(boundingBox);
        final List<GeoHash> searchHashes = geoHashBoundingBoxQuery.getSearchHashes();
        for (final GeoHash geoHash : searchHashes)
        {
            doScan(geoHash, lon, lat, boundingBox, columnList);
        }
        return m_found;
    }

    private void doScan(
            final GeoHash start,
            final double origLon,
            final double origLat,
            final BoundingBox boundingBox,
            final List<ColumnInterface> columnList) throws IOException
    {
        final Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start.longValue()));
        scan.setStopRow(Bytes.toBytes(start.next().longValue()));
        scan.setMaxVersions(1);
        scan.setCaching(m_scanCaching);
        scan.setFilter(new BoundingBoxFilter(boundingBox));
        for (final ColumnInterface column : columnList)
        {
            scan.addColumn(column.getFamilyAsBytes(), column.getQualifierAsBytes());
        }
        final ResultScanner scanner = m_table.getScanner(scan);
        try
        {
            for (final Result result : scanner)
            {
                final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.getRow());
                final DataInput dataInput = new DataInputStream(byteArrayInputStream);
                final long bits = dataInput.readLong();
                final double resultLon = dataInput.readDouble();
                final double resultLat = dataInput.readDouble();
                // Dummy implementation of geo distance - should use http://en.wikipedia.org/wiki/Vincenty's_formulae
                final double deltaLon = resultLon - origLon;
                final double deltaLat = resultLat - origLat;
                final double distance = Math.sqrt(deltaLon * deltaLon + deltaLat * deltaLat);
                final double weight = 1.0 - Math.min(m_buffer, distance) / m_buffer;
                if (weight > 0.0)
                {
                    m_found = true;
                    for (final ColumnInterface column : columnList)
                    {
                        final double value = column.toDouble(result.getValue(column.getFamilyAsBytes(), column.getQualifierAsBytes()));
                        column.addWeight(weight * value);
                    }
                }
            }
        }
        finally
        {
            scanner.close();
        }
    }

    @Override
    public void cleanup(final Configuration configuration) throws IOException
    {
        if (m_table != null)
        {
            m_table.close();
        }
    }
}
