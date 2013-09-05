package com.esri;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.junit.Assert;
import org.junit.Test;

/**
 * Simple test of encoding/decoding to/from Esri binary shape
 */
public class GeomEngineTest
{
    @Test
    public void testGeomEngine()
    {
        final Envelope orig = new Envelope(0, 0, 10, 10);
        final byte[] bytes = GeometryEngine.geometryToEsriShape(orig);
        Assert.assertNotNull(bytes);
        final Geometry geometry = GeometryEngine.geometryFromEsriShape(bytes, Geometry.Type.Envelope);
        Assert.assertNotNull(geometry);
        Assert.assertTrue(geometry instanceof Envelope);
        final Envelope dest = (Envelope) geometry;
        Assert.assertEquals(0, dest.getXMin(), 0.001);
        Assert.assertEquals(0, dest.getYMin(), 0.001);
        Assert.assertEquals(10, dest.getXMax(), 0.001);
        Assert.assertEquals(10, dest.getYMax(), 0.001);
    }
}