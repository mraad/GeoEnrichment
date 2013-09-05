package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 */
public final class GeoEnrichmentJob extends Configured implements Tool
{
    public final static String KEY_INPUT_SEPARATOR = "com.esri.inputSeparator";
    public final static String KEY_OUTPUT_SEPARATOR = "com.esri.outputSeparator";
    public final static String KEY_LON_FIELD = "com.esri.lonField";
    public final static String KEY_LAT_FIELD = "com.esri.latField";
    public final static String KEY_BUFFER = "com.esri.buffer";
    public static final String KEY_TABLE = "com.esri.table";
    public static final String KEY_COLUMNS = "com.esri.columns";
    public static final String KEY_SEARCH_CLASS = "com.esri.searchClass";
    public static final String KEY_WRITE_ALL = "com.esri.writeAll";

    @Override
    public int run(final String[] args) throws Exception
    {
        final Configuration conf = getConf();
        setConf(HBaseConfiguration.create(conf));
        final String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2)
        {
            System.err.println("Usage:\n\tbin/hadoop jar [genericOptions] input-path output-path [shapfile-path#shapefile]\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        if (isKeyMissing(getConf(), KEY_LON_FIELD))
        {
            return -1;
        }
        if (isKeyMissing(getConf(), KEY_LAT_FIELD))
        {
            return -1;
        }
        if (isKeyMissing(getConf(), KEY_COLUMNS))
        {
            return -1;
        }
        if (isKeyMissing(getConf(), KEY_SEARCH_CLASS))
        {
            return -1;
        }
        return createSubmittableJob(conf, remainingArgs).waitForCompletion(true) ? 0 : 1;
    }

    private boolean isKeyMissing(
            final Configuration configuration,
            final String key)
    {
        if (configuration.get(key) == null)
        {
            System.err.format("Configuration key '%s' is missing.\n", key);
            return true;
        }
        return false;
    }

    public static Job createSubmittableJob(
            final Configuration conf,
            final String[] args) throws IOException, URISyntaxException
    {
        final Job job = Job.getInstance(conf, GeoEnrichmentJob.class.getSimpleName());
        job.setJarByClass(GeoEnrichmentJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GeoEnrichmentMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (args.length == 3)
        {
            DistributedCache.createSymlink(job.getConfiguration());
            DistributedCache.addCacheArchive(new URI(args[2] + "#shapefile"), job.getConfiguration());
        }
        return job;
    }

    public static void main(String[] args) throws Exception
    {
        final int status = ToolRunner.run(new GeoEnrichmentJob(), args);
        System.exit(status);
    }

}
