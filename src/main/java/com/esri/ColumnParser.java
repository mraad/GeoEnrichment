package com.esri;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class ColumnParser
{
    private final Log m_log = LogFactory.getLog(getClass());

    public ColumnParser()
    {
    }

    public static ColumnParser newInstance()
    {
        return new ColumnParser();
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

    @Deprecated
    public List<ColumnInterface> parseColumns(final String text)
    {
        final List<ColumnInterface> list = new ArrayList<ColumnInterface>();
        for (final String item : text.split(","))
        {
            parseColumn(list, item);
        }
        if (list.size() == 0)
        {
            parseColumn(list, text);
        }
        return list;
    }

    public List<ColumnInterface> parseColumn(final String text)
    {
        return parseColumns(new String[]{text});
    }

    private void parseColumn(
            final List<ColumnInterface> list,
            final String text)
    {
        final String[] items = text.split("\\|");
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
            m_log.warn("Expecting 3 or 4 items in '" + text + "'when split by |");
        }
    }

}
