package com.github.susom.database;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Date;

/**
 * Interface for reading results from a database query.
 *
 * @author garricko
 */
public interface Rows {
  boolean next();

  Integer getInteger(int columnOneBased);

  Integer getInteger(String columnName);

  Long getLong(int columnOneBased);

  Long getLong(String columnName);

  Float getFloat(int columnOneBased);

  Float getFloat(String columnName);

  Double getDouble(int columnOneBased);

  Double getDouble(String columnName);

  /**
   * Note this method attempts to correct for "artifical" scale due to the database
   * representation. Some databases will pad the number out to "full precision". This
   * method tries to reduce scale if there is zero padding to the right of the decimal.
   */
  BigDecimal getBigDecimal(int columnOneBased);

  BigDecimal getBigDecimal(String columnName);

  String getString(int columnOneBased);

  String getString(String columnName);

  String getClobString(int columnOneBased);

  String getClobString(String columnName);

  Reader getClobReader(int columnOneBased);

  Reader getClobReader(String columnName);

  byte[] getBlobBytes(int columnOneBased);

  byte[] getBlobBytes(String columnName);

  InputStream getBlobInputStream(int columnOneBased);

  InputStream getBlobInputStream(String columnName);

  /**
   * Return the millisecond precision Date, which should be represented as a TIMESTAMP
   * in the database. The nanoseconds are truncated.
   */
  Date getDate(int columnOneBased);

  Date getDate(String columnName);
}
