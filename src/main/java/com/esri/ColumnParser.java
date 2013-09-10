package com.esri;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 */
public class ColumnParser
{
    private final Log m_log = LogFactory.getLog(getClass());
    private final Pattern m_pattern = Pattern.compile(":");

    public ColumnParser()
    {
    }

    public static ColumnParser newInstance()
    {
        return new ColumnParser();
    }

    public List<ColumnInterface> parseColumn(final String text)
    {
        return parseColumns(new String[]{text});
    }

    public List<ColumnInterface> parseColumns(final String[] textArr)
    {
        final List<ColumnInterface> list = new ArrayList<ColumnInterface>();
        for (final String text : textArr)
        {
            parseColumn(list, text);
        }
        return list;
    }

    private void parseColumn(
            final List<ColumnInterface> list,
            final String text)
    {
        final String[] items = m_pattern.split(text);
        if (items.length == 3)
        {
            list.add(new ColumnDouble(items[0], items[1], items[2]));
        }
        else if (items.length == 4)
        {
            switch (items[3].charAt(0))
            {
                case 'f':
                    list.add(new ColumnFloat(items[0], items[1], items[2]));
                    break;
                case 'l':
                    list.add(new ColumnLong(items[0], items[1], items[2]));
                    break;
                case 'i':
                    list.add(new ColumnInt(items[0], items[1], items[2]));
                    break;
                default:
                    list.add(new ColumnDouble(items[0], items[1], items[2]));
            }
        }
        else
        {
            m_log.warn("Expecting 3 or 4 items in '" + text + "' when split by comma");
        }
    }

}
