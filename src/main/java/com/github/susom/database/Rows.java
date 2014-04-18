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
