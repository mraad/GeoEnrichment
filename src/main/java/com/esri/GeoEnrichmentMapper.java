package com.esri;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 */
public class GeoEnrichmentMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
    public static final String BAD_LINE = "Bad Line";

    private final Log m_log = LogFactory.getLog(getClass());

    private final NullWritable m_nullWritable = NullWritable.get();

    private int m_lonField;
    private int m_latField;
    private Pattern m_pattern;
    private Counter m_badLineCounter;
    private List<ColumnInterface> m_columnList;
    private String m_outputSeparator;
    private SearchInterface m_searchInterface;
    private String m_inputSeparator;
    private boolean m_writeAll;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();

        m_inputSeparator = configuration.get(GeoEnrichmentJob.KEY_INPUT_SEPARATOR, "\t");
        m_outputSeparator = configuration.get(GeoEnrichmentJob.KEY_OUTPUT_SEPARATOR, m_inputSeparator);
        m_pattern = Pattern.compile(m_inputSeparator);
        m_writeAll = configuration.getBoolean(GeoEnrichmentJob.KEY_WRITE_ALL, true);
        m_lonField = configuration.getInt(GeoEnrichmentJob.KEY_LON_FIELD, 0);
        m_latField = configuration.getInt(GeoEnrichmentJob.KEY_LAT_FIELD, 0);
        m_columnList = ColumnParser.newInstance().parseColumns(configuration.getStrings(GeoEnrichmentJob.KEY_COLUMN));

        final Class<SearchInterface> clazz = (Class<SearchInterface>) configuration.getClass(GeoEnrichmentJob.KEY_SEARCH_CLASS, SearchNoop.class);
        try
        {
            m_searchInterface = clazz.newInstance();
        }
        catch (Exception e)
        {
            m_log.warn(e.toString(), e);
            m_searchInterface = new SearchNoop();
        }
        m_searchInterface.setup(configuration, m_columnList);

        m_badLineCounter = context.getCounter(GeoEnrichmentMapper.class.getSimpleName(), BAD_LINE);
    }

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context) throws IOException, InterruptedException
    {
        final String text = value.toString();
        final String[] tokens = m_pattern.split(text);
        try
        {
            final double lon = Double.parseDouble(tokens[m_lonField]);
            final double lat = Double.parseDouble(tokens[m_latField]);
            final boolean found = m_searchInterface.search(lon, lat, m_columnList);
            if (m_writeAll || found)
            {
                final StringBuffer stringBuffer = new StringBuffer();
                if (m_inputSeparator.equals(m_outputSeparator))
                {
                    stringBuffer.append(text);
                }
                else
                {
                    stringBuffer.append(text.replaceAll(m_inputSeparator, m_outputSeparator));
                }
                for (final ColumnInterface column : m_columnList)
                {
                    stringBuffer.append(m_outputSeparator).append(column.toFormattedString());
                }
                context.write(m_nullWritable, new Text(stringBuffer.toString()));
            }
        }
        catch (final Throwable t)
        {
            m_badLineCounter.increment(1L);
            m_log.error(t.toString());
        }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException
    {
        m_searchInterface.cleanup(context.getConfiguration());
    }
}
