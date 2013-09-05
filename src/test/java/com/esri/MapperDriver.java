package com.esri;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

/**
 */
final class MapperDriver extends MapDriver<LongWritable, Text, NullWritable, Text>
{
    public static MapperDriver newMapperDriver(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NullWritable, Text> mapper)
    {
        final MapperDriver mapperDriver = new MapperDriver();
        mapperDriver.setMapper(mapper);
        return mapperDriver;
    }

    public MapperDriver configure(
            final String key,
            final String val)
    {
        getConfiguration().set(key, val);
        return this;
    }

    public MapperDriver configure(
            final String name,
            final Class<?> aClass,
            final Class<SearchInterface> anInterface)
    {
        getConfiguration().setClass(name, aClass, anInterface);
        return this;
    }

    public MapperDriver configure(
            final String key,
            final float value)
    {
        getConfiguration().setFloat(key, value);
        return this;
    }
}
