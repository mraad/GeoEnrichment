package com.esri;

import ch.hsr.geohash.BoundingBox;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 */
public class BoundingBoxFilterTest
{
    @Test
    public void testFilterRowKey() throws Exception
    {
        final BoundingBox boundingBox = new BoundingBox(-10, 10, -5, 5);
        final BoundingBoxFilter boundingBoxFilter = new BoundingBoxFilter(boundingBox);

        byte[] bytes;
        final QuadRowKey quadRowKey = new QuadRowKey();

        bytes = quadRowKey.toBytes(0, 0);
        Assert.assertFalse(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(-5, -10);
        Assert.assertFalse(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(-5, 10);
        Assert.assertFalse(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(5, -10);
        Assert.assertFalse(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(5, 10);
        Assert.assertFalse(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(-6, -11);
        Assert.assertTrue(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(-6, 11);
        Assert.assertTrue(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(6, -11);
        Assert.assertTrue(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));

        bytes = quadRowKey.toBytes(6, 11);
        Assert.assertTrue(boundingBoxFilter.filterRowKey(bytes, 0, bytes.length));
    }

    @Test
    public void testWriteRead() throws Exception
    {
        final BoundingBox boundingBoxOld = new BoundingBox(-10, 10, -5, 5);
        final BoundingBoxFilter boundingBoxFilter = new BoundingBoxFilter(boundingBoxOld);
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        boundingBoxFilter.write(new DataOutputStream(byteArrayOutputStream));

        boundingBoxFilter.readFields(new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));
        final BoundingBox boundingBox = boundingBoxFilter.getBoundingBox();

        Assert.assertEquals(boundingBoxOld.getMinLat(), boundingBox.getMinLat(), 0.000001);
        Assert.assertEquals(boundingBoxOld.getMinLon(), boundingBox.getMinLon(), 0.000001);
        Assert.assertEquals(boundingBoxOld.getMaxLat(), boundingBox.getMaxLat(), 0.000001);
        Assert.assertEquals(boundingBoxOld.getMaxLon(), boundingBox.getMaxLon(), 0.000001);

    }

}
