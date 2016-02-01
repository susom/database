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
  SqlInsert argDate(Date arg);

  @Nonnull
  @CheckReturnValue
  SqlInsert argDate(@Nonnull String argName, Date arg);

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
   * setting any parameters, after setting all, or multiple times between rows.
   */
//  SqlInsert batch();

  @CheckReturnValue
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
