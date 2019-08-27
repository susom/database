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
import java.time.LocalDate;
import java.util.Date;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlInsert {
  @Nonnull
  @CheckReturnValue
  SqlInsert argBoolean(Boolean arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argBoolean(@Nonnull String argName, Boolean arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argInteger(Integer arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argInteger(@Nonnull String argName, Integer arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argLong(Long arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argLong(@Nonnull String argName, Long arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argFloat(Float arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argFloat(@Nonnull String argName, Float arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argDouble(Double arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argDouble(@Nonnull String argName, Double arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argBigDecimal(BigDecimal arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argBigDecimal(@Nonnull String argName, BigDecimal arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argString(String arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argString(@Nonnull String argName, String arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argDate(Date arg); // date with time

  @Nonnull
  @CheckReturnValue
  SqlInsert argDate(@Nonnull String argName, Date arg); // date with time

  @Nonnull
  @CheckReturnValue
  SqlInsert argLocalDate(LocalDate arg); // date only - no timestamp

  @Nonnull
  @CheckReturnValue
  SqlInsert argLocalDate(@Nonnull String argName, LocalDate arg);  // date only - no timestamp

  @Nonnull
  @CheckReturnValue
  SqlInsert argDateNowPerApp();

  @Nonnull
  @CheckReturnValue
  SqlInsert argDateNowPerApp(@Nonnull String argName);

  @Nonnull
  @CheckReturnValue
  SqlInsert argDateNowPerDb();

  @Nonnull
  @CheckReturnValue
  SqlInsert argDateNowPerDb(@Nonnull String argName);

  @Nonnull
  @CheckReturnValue
  SqlInsert argBlobBytes(byte[] arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argBlobBytes(@Nonnull String argName, byte[] arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argBlobStream(InputStream arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argBlobStream(@Nonnull String argName, InputStream arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argClobString(String arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argClobString(@Nonnull String argName, String arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argClobReader(Reader arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argClobReader(@Nonnull String argName, Reader arg);

  interface Apply {
    void apply(SqlInsert insert);
  }

  @Nonnull
  @CheckReturnValue
  SqlInsert withArgs(SqlArgs args);

  @Nonnull
  @CheckReturnValue
  SqlInsert apply(Apply apply);

  /**
   * Call this between setting rows of parameters for a SQL statement. You may call it before
   * setting any parameters, after setting all, or multiple times between rows. This feature
   * only currently works with basic inserts (you can't do insertReturning type operations).
   */
  SqlInsert batch();

  /**
   * Perform the insert into the database without any verification of how many rows
   * were affected.
   *
   * @return the number of rows affected
   */
  int insert();

  /**
   * Perform the insert into the database. This will automatically verify
   * that the specified number of rows was affected, and throw a {@link WrongNumberOfRowsException}
   * if it does not match.
   */
  void insert(int expectedRowsUpdated);

  /**
   * Insert multiple rows in one database call. This will automatically verify
   * that exactly 1 row is affected for each row of parameters.
   */
  void insertBatch();

  /**
   * Insert multiple rows in one database call. This returns the results for
   * each row so you can check them yourself.
   *
   * @return an array with an element for each row in the batch; the value
   *         of each array indicates how many rows were affected; note that
   *         some database/driver combinations do now return this information
   *         (for example, older versions of Oracle return -2 rather than the
   *         number of rows)
   */
  int[] insertBatchUnchecked();

  /**
   * Use this method in conjunction with argPkSeq() to optimize inserts where the
   * primary key is being populated from a database sequence at insert time. If the
   * database can't support this feature it will be simulated with a select and then
   * the insert.
   *
   * <p>This version of insert expects exactly one row to be inserted, and will throw
   * a DatabaseException if that isn't the case.</p>
   */
  @CheckReturnValue
  Long insertReturningPkSeq(String primaryKeyColumnName);

  <T> T insertReturning(String tableName, String primaryKeyColumnName, RowsHandler<T> rowsHandler,
                        String...otherColumnNames);

  @Nonnull
  @CheckReturnValue
  SqlInsert argPkSeq(@Nonnull String sequenceName);

  /**
   * Use this method to populate the primary key value (assumed to be type Long)
   * from a sequence in the database. This can be used standalone, but is intended
   * to be used in conjunction with insertReturningPkSeq() to both insert and obtain
   * the inserted value in an optimized way (if possible). For databases that are
   * unable to return the value from the insert (such as Derby) this will be simulated
   * first issuing a select to read the sequence, then an insert.
   */
  @Nonnull
  @CheckReturnValue
  SqlInsert argPkSeq(@Nonnull String argName, @Nonnull String sequenceName);

  @Nonnull
  @CheckReturnValue
  SqlInsert argPkLong(Long pkValue);

  @Nonnull
  @CheckReturnValue
  SqlInsert argPkLong(String argName, Long pkValue);
}
