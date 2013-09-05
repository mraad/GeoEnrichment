package com.esri;

import ch.hsr.geohash.BoundingBox;

/**
 */
public final class Quad
{
    private Quad()
    {
    }

    private final static long MSB = 1L << 63;

    public static BoundingBox decode(final long bits)
    {
        return decode(bits, 32);
    }

    public static BoundingBox decode(
            long bits,
            int levels)
    {
        double xmin = -180, xmax = 180, ymin = -90, ymax = 90;
        levels = Math.min(32, levels);
        for (int l = 0; l < levels; l++)
        {
            final double xmid = (xmin + xmax) * 0.5;
            if ((bits & MSB) == 0)
            {
                xmax = xmid;
            }
            else
            {
                xmin = xmid;
            }
            bits <<= 1;
            final double ymid = (ymin + ymax) * 0.5;
            if ((bits & MSB) == 0)
            {
                ymax = ymid;
            }
            else
            {
                ymin = ymid;
            }
            bits <<= 1;
        }
        return new BoundingBox(ymin, ymax, xmin, xmax);
    }

    public static long encode(
            final double x,
            final double y
    )
    {
        return encode(x, y, 32);
    }

    public static long encode(
            final double x,
            final double y,
            int levels)
    {
        long bits = 0;

        double xmin = -180, xmax = 180, ymin = -90, ymax = 90;

        levels = Math.min(32, levels);

        for (int l = 0; l < levels; l++)
        {
            bits <<= 1;
            final double xmid = (xmin + xmax) * 0.5;
            if (x < xmid)
            {
                xmax = xmid;
            }
            else
            {
                xmin = xmid;
                bits |= 1L;
            }
            bits <<= 1;
            final double ymid = (ymin + ymax) * 0.5;
            if (y < ymid)
            {
                ymax = ymid;
            }
            else
            {
                ymin = ymid;
                bits |= 1L;
            }
        }
        bits <<= (64 - levels - levels);
        return bits;
    }
}
