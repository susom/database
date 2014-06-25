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
import java.util.Date;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for reading results from a database query.
 *
 * @author garricko
 */
public interface Rows {
  boolean next();

  @Nullable
  Integer getIntegerOrNull(int columnOneBased);

  @Nullable
  Integer getIntegerOrNull(String columnName);

  int getIntegerOrZero(int columnOneBased);

  int getIntegerOrZero(String columnName);

  @Nullable
  Long getLongOrNull(int columnOneBased);

  @Nullable
  Long getLongOrNull(String columnName);

  long getLongOrZero(int columnOneBased);

  long getLongOrZero(String columnName);

  @Nullable
  Float getFloatOrNull(int columnOneBased);

  @Nullable
  Float getFloatOrNull(String columnName);

  float getFloatOrZero(int columnOneBased);

  float getFloatOrZero(String columnName);

  @Nullable
  Double getDoubleOrNull(int columnOneBased);

  @Nullable
  Double getDoubleOrNull(String columnName);

  double getDoubleOrZero(int columnOneBased);

  double getDoubleOrZero(String columnName);

  /**
   * Note this method attempts to correct for "artifical" scale due to the database
   * representation. Some databases will pad the number out to "full precision". This
   * method tries to reduce scale if there is zero padding to the right of the decimal.
   */
  @Nullable
  BigDecimal getBigDecimalOrNull(int columnOneBased);

  @Nullable
  BigDecimal getBigDecimalOrNull(String columnName);

  @NotNull
  BigDecimal getBigDecimalOrZero(int columnOneBased);

  @NotNull
  BigDecimal getBigDecimalOrZero(String columnName);

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
  @NotNull
  String getStringOrEmpty(int columnOneBased);

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @NotNull
  String getStringOrEmpty(String columnName);

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
  @NotNull
  String getClobStringOrEmpty(int columnOneBased);

  /**
   * @return the value, or the empty string if it is SQL null; never returns null
   */
  @NotNull
  String getClobStringOrEmpty(String columnName);

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
  @NotNull
  Reader getClobReaderOrEmpty(int columnOneBased);

  /**
   * @return the value, or a StringReader containing the empty string if it is SQL null
   */
  @NotNull
  Reader getClobReaderOrEmpty(String columnName);

  @Nullable
  byte[] getBlobBytesOrNull(int columnOneBased);

  @Nullable
  byte[] getBlobBytesOrNull(String columnName);

  @NotNull
  byte[] getBlobBytesOrZeroLen(int columnOneBased);

  @NotNull
  byte[] getBlobBytesOrZeroLen(String columnName);

  @Nullable
  InputStream getBlobInputStreamOrNull(int columnOneBased);

  @Nullable
  InputStream getBlobInputStreamOrNull(String columnName);

  @NotNull
  InputStream getBlobInputStreamOrEmpty(int columnOneBased);

  @NotNull
  InputStream getBlobInputStreamOrEmpty(String columnName);

  /**
   * Return the millisecond precision Date, which should be represented as a TIMESTAMP
   * in the database. The nanoseconds are truncated.
   */
  @Nullable
  Date getDateOrNull(int columnOneBased);

  @Nullable
  Date getDateOrNull(String columnName);
}
