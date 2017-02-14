/*
 * Copyright 2014 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.susom.database;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.util.Date;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for reading results from a database query.
 *
 * @author garricko
 */
public interface Row {
  /**
   * Obtain the names of the columns in the database. You probably want to
   * avoid this method if possible, as the way column names are handled varies
   * by database and driver. For example, Derby and Oracle normally convert
   * column names to uppercase, while PostgreSQL normally converts column
   * names to lowercase. If you do use this method, you might want to either
   * call toUppercase()/toLowercase() or ensure the SQL explicitly specifies
   * parameters with AS "FOO" (including quotes) to ensure your desired name
   * will be honored.
   */
  @Nonnull
  String[] getColumnLabels();

  /**
   * Get raw access to the underlying JDBC metadata.
   */
  @Nonnull
  ResultSetMetaData getMetadata();

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to {@code Boolean} or {@code null} as appropriate.
   *
   * <p>This is a short-hand method that reads columns in order, starting
   * with the first, and automatically incrementing the column index.</p>
   *
   * <p>If you call one of the methods using an explicit column index or column name before
   * calling this method, it will pick up at the next column following the explicit one.
   * For example:</p>
   *
   * <pre>
   * getX();  // column 1
   * getX(5); // or getX("foo") if foo is column 5
   * getX();  // column 6
   * </pre>
   *
   * @return true if the value was "Y", false if it was "N", or null
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  @Nullable
  Boolean getBooleanOrNull();

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to {@code Boolean} or {@code null} as appropriate.
   *
   * @param columnOneBased column number to read (1 is the first column)
   * @return true if the value was "Y", false if it was "N", or null
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  @Nullable
  Boolean getBooleanOrNull(int columnOneBased);

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to {@code Boolean} or {@code null} as appropriate.
   *
   * @param columnName SQL alias of the column to read (use all lowercase)
   * @return true if the value was "Y", false if it was "N", or null
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  @Nullable
  Boolean getBooleanOrNull(String columnName);

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to a {@code boolean}. If the value is {@code null}, it will be converted to {@code false}.
   *
   * <p>This is a short-hand method that reads columns in order, starting
   * with the first, and automatically incrementing the column index.</p>
   *
   * <p>If you call one of the methods using an explicit column index or column name before
   * calling this method, it will pick up at the next column following the explicit one.
   * For example:</p>
   *
   * <pre>
   * getX();  // column 1
   * getX(5); // or getX("foo") if foo is column 5
   * getX();  // column 6
   * </pre>
   *
   * @return true if the value was "Y", false if it was either "N" or null
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  boolean getBooleanOrFalse();

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to a {@code boolean}. If the value is {@code null}, it will be converted to {@code false}.
   *
   * <p>This is a short-hand method that reads columns in order, starting
   * with the first, and automatically incrementing the column index.</p>
   *
   * @param columnOneBased column number to read (1 is the first column)
   * @return true if the value was "Y", false if it was either "N" or null
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  boolean getBooleanOrFalse(int columnOneBased);

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to a {@code boolean}. If the value is {@code null}, it will be converted to {@code false}.
   *
   * <p>This is a short-hand method that reads columns in order, starting
   * with the first, and automatically incrementing the column index.</p>
   *
   * @param columnName SQL alias of the column to read (use all lowercase)
   * @return true if the value was "Y", false if it was either "N" or null
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  boolean getBooleanOrFalse(String columnName);

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to a {@code boolean}. If the value is {@code null}, it will be converted to {@code true}.
   *
   * <p>This is a short-hand method that reads columns in order, starting
   * with the first, and automatically incrementing the column index.</p>
   *
   * <p>If you call one of the methods using an explicit column index or column name before
   * calling this method, it will pick up at the next column following the explicit one.
   * For example:</p>
   *
   * <pre>
   * getX();  // column 1
   * getX(5); // or getX("foo") if foo is column 5
   * getX();  // column 6
   * </pre>
   *
   * @return true if the value was either "Y" or null, false if it was "N"
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  boolean getBooleanOrTrue();

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to a {@code boolean}. If the value is {@code null}, it will be converted to {@code true}.
   *
   * <p>This is a short-hand method that reads columns in order, starting
   * with the first, and automatically incrementing the column index.</p>
   *
   * @param columnOneBased column number to read (1 is the first column)
   * @return true if the value was either "Y" or null, false if it was "N"
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  boolean getBooleanOrTrue(int columnOneBased);

  /**
   * Boolean values are represented as strings {@code "Y"} or {@code "N"} in the database,
   * typically in a {@code CHAR(1)} column. This reads the value and converts it
   * to a {@code boolean}. If the value is {@code null}, it will be converted to {@code true}.
   *
   * <p>This is a short-hand method that reads columns in order, starting
   * with the first, and automatically incrementing the column index.</p>
   *
   * @param columnName SQL alias of the column to read (use all lowercase)
   * @return true if the value was either "Y" or null, false if it was "N"
   * @throws DatabaseException if the value was something other than Y, N, or null
   */
  boolean getBooleanOrTrue(String columnName);

  @Nullable
  Integer getIntegerOrNull();

  @Nullable
  Integer getIntegerOrNull(int columnOneBased);

  @Nullable
  Integer getIntegerOrNull(String columnName);

  int getIntegerOrZero();

  int getIntegerOrZero(int columnOneBased);

  int getIntegerOrZero(String columnName);

  @Nullable
  Long getLongOrNull();

  @Nullable
  Long getLongOrNull(int columnOneBased);

  @Nullable
  Long getLongOrNull(String columnName);

  long getLongOrZero();

  long getLongOrZero(int columnOneBased);

  long getLongOrZero(String columnName);

  @Nullable
  Float getFloatOrNull();

  @Nullable
  Float getFloatOrNull(int columnOneBased);

  @Nullable
  Float getFloatOrNull(String columnName);

  float getFloatOrZero();

  float getFloatOrZero(int columnOneBased);

  float getFloatOrZero(String columnName);

  @Nullable
  Double getDoubleOrNull();

  @Nullable
  Double getDoubleOrNull(int columnOneBased);

  @Nullable
  Double getDoubleOrNull(String columnName);

  double getDoubleOrZero();

  double getDoubleOrZero(int columnOneBased);

  double getDoubleOrZero(String columnName);

  /**
   * Note this method attempts to correct for "artifical" scale due to the database
   * representation. Some databases will pad the number out to "full precision". This
   * method tries to reduce scale if there is zero padding to the right of the decimal.
   */
  @Nullable
  BigDecimal getBigDecimalOrNull();

  @Nullable
  BigDecimal getBigDecimalOrNull(int columnOneBased);

  @Nullable
  BigDecimal getBigDecimalOrNull(String columnName);

  @Nonnull
  BigDecimal getBigDecimalOrZero();

  @Nonnull
  BigDecimal getBigDecimalOrZero(int columnOneBased);

  @Nonnull
  BigDecimal getBigDecimalOrZero(String columnName);

  /**
   * @return the value, or null if it is SQL null; never returns the empty string
   */
  @Nullable
  String getStringOrNull();

  /**
   * @return the value, or null if it is SQL null; never returns the empty string
   */
  @Nullable
  String getStringOrNull(int columnOneBased);

  /**
   * @return the value, or null if it is SQL null; never returns the empty string
   */
  @Nullable
  String getStringOrNull(String columnName);

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @Nonnull
  String getStringOrEmpty();

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @Nonnull
  String getStringOrEmpty(int columnOneBased);

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @Nonnull
  String getStringOrEmpty(String columnName);

  /**
   * @return the value, or null if it is SQL null; never returns the empty string
   */
  @Nullable
  String getClobStringOrNull();

  /**
   * @return the value, or null if it is SQL null; never returns the empty string
   */
  @Nullable
  String getClobStringOrNull(int columnOneBased);

  /**
   * @return the value, or null if it is SQL null; never returns the empty string
   */
  @Nullable
  String getClobStringOrNull(String columnName);

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @Nonnull
  String getClobStringOrEmpty();

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @Nonnull
  String getClobStringOrEmpty(int columnOneBased);

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @Nonnull
  String getClobStringOrEmpty(String columnName);

  /**
   * @return the value, or null if it is SQL null
   */
  @Nullable
  Reader getClobReaderOrNull();

  /**
   * @return the value, or null if it is SQL null
   */
  @Nullable
  Reader getClobReaderOrNull(int columnOneBased);

  /**
   * @return the value, or null if it is SQL null
   */
  @Nullable
  Reader getClobReaderOrNull(String columnName);

  /**
   * @return the value, or a StringReader containing the empty string if it is SQL null
   */
  @Nonnull
  Reader getClobReaderOrEmpty();

  /**
   * @return the value, or a StringReader containing the empty string if it is SQL null
   */
  @Nonnull
  Reader getClobReaderOrEmpty(int columnOneBased);

  /**
   * @return the value, or a StringReader containing the empty string if it is SQL null
   */
  @Nonnull
  Reader getClobReaderOrEmpty(String columnName);

  @Nullable
  byte[] getBlobBytesOrNull();

  @Nullable
  byte[] getBlobBytesOrNull(int columnOneBased);

  @Nullable
  byte[] getBlobBytesOrNull(String columnName);

  @Nonnull
  byte[] getBlobBytesOrZeroLen();

  @Nonnull
  byte[] getBlobBytesOrZeroLen(int columnOneBased);

  @Nonnull
  byte[] getBlobBytesOrZeroLen(String columnName);

  @Nullable
  InputStream getBlobInputStreamOrNull();

  @Nullable
  InputStream getBlobInputStreamOrNull(int columnOneBased);

  @Nullable
  InputStream getBlobInputStreamOrNull(String columnName);

  @Nonnull
  InputStream getBlobInputStreamOrEmpty();

  @Nonnull
  InputStream getBlobInputStreamOrEmpty(int columnOneBased);

  @Nonnull
  InputStream getBlobInputStreamOrEmpty(String columnName);

  /**
   * Return the millisecond precision Date, which should be represented as a TIMESTAMP
   * in the database. The nanoseconds are truncated.
   */
  @Nullable
  Date getDateOrNull();

  /**
   * Return the millisecond precision Date, which should be represented as a TIMESTAMP
   * in the database. The nanoseconds are truncated.
   */
  @Nullable
  Date getDateOrNull(int columnOneBased);

  @Nullable
  Date getDateOrNull(String columnName);
}
