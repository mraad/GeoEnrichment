package com.esri;

/**
 */
public abstract class ColumnAbstract implements ColumnInterface
{
    protected final String m_family;
    protected final String m_qualifier;
    protected byte[] m_familyAsBytes;
    protected byte[] m_qualifierAsBytes;
    protected String m_format;
    protected double m_weight;
    protected double m_count;

    public ColumnAbstract(
            final String family,
            final String qualifier,
            final String format)
    {
        m_family = family;
        m_qualifier = qualifier;
        m_familyAsBytes = family.getBytes();
        m_qualifierAsBytes = qualifier.getBytes();
        m_format = format;
    }

    @Override
    public void reset()
    {
        m_weight = 0.0;
        m_count = 0.0;
    }

    public String getFamily()
    {
        return m_family;
    }

    public String getQualifier()
    {
        return m_qualifier;
    }

    @Override
    public byte[] getFamilyAsBytes()
    {
        return m_familyAsBytes;
    }

    @Override
    public byte[] getQualifierAsBytes()
    {
        return m_qualifierAsBytes;
    }

    @Override
    public double getValue()
    {
        return m_count == 0.0 ? 0.0 : m_weight / m_count;
    }

    @Override
    public String toFormattedString()
    {
        return String.format(m_format, getValue());
    }

    @Override
    public void addWeight(final double weight)
    {
        m_weight += weight;
        m_count += 1.0;
    }

    @Override
    public void setWeight(final double weight)
    {
        m_weight = weight;
        m_count = 1.0;
    }

}
