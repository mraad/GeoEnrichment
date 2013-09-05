package com.esri;

/**
 */
public interface ColumnInterface
{
    void reset();

    String getFamily();

    String getQualifier();

    byte[] getFamilyAsBytes();

    byte[] getQualifierAsBytes();

    double getValue();

    double toDouble(final byte[] bytes);

    double toDouble(final Object obj);

    String toFormattedString();

    void addWeight(final double weight);

    void setWeight(final double weight);
}
