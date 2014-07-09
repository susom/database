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
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlInsert {
  SqlInsert argInteger(Integer arg);

  SqlInsert argInteger(String argName, Integer arg);

  SqlInsert argLong(Long arg);

  SqlInsert argLong(String argName, Long arg);

  SqlInsert argFloat(Float arg);

  SqlInsert argFloat(String argName, Float arg);

  SqlInsert argDouble(Double arg);

  SqlInsert argDouble(String argName, Double arg);

  SqlInsert argBigDecimal(BigDecimal arg);

  SqlInsert argBigDecimal(String argName, BigDecimal arg);

  SqlInsert argString(String arg);

  SqlInsert argString(String argName, String arg);

  SqlInsert argDate(Date arg);

  SqlInsert argDate(String argName, Date arg);

  SqlInsert argDateNowPerApp(String argName);

  SqlInsert argDateNowPerDb(String argName);

  SqlInsert argBlobBytes(byte[] arg);

  SqlInsert argBlobBytes(String argName, byte[] arg);

  SqlInsert argBlobStream(InputStream arg);

  SqlInsert argBlobStream(String argName, InputStream arg);

  SqlInsert argClobString(String arg);

  SqlInsert argClobString(String argName, String arg);

  SqlInsert argClobReader(Reader arg);

  SqlInsert argClobReader(String argName, Reader arg);

  /**
   * Call this between setting rows of parameters for a SQL statement. You may call it before
   * setting any parameters, after setting all, or multiple times between rows.
   */
//  SqlInsert batch();

  int insert();

  void insert(int expectedRowsUpdated);

  /**
   * Use this method in conjunction with argPkSeq() to optimize inserts where the
   * primary key is being populated from a database sequence at insert time. If the
   * database can't support this feature it will be simulated with a select and then
   * the insert.
   *
   * <p>This version of insert expects exactly one row to be inserted, and will throw
   * a DatabaseException if that isn't the case.</p>
   */
  Long insertReturningPkSeq(String primaryKeyColumnName);

  <T> T insertReturning(String tableName, String primaryKeyColumnName, RowsHandler<T> rowsHandler,
                        String...otherColumnNames);

//  SqlInsert argPkSeq(String sequenceName);

  /**
   * Use this method to populate the primary key value (assumed to be type Long)
   * from a sequence in the database. This can be used standalone, but is intended
   * to be used in conjunction with insertReturningPkSeq() to both insert and obtain
   * the inserted value in an optimized way (if possible). For databases that are
   * unable to return the value from the insert (such as Derby) this will be simulated
   * first issuing a select to read the sequence, then an insert.
   */
  SqlInsert argPkSeq(String argName, String sequenceName);
  SqlInsert argPkLong(Long pkValue);
  SqlInsert argPkLong(String argName, Long pkValue);
}
