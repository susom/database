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

import org.jetbrains.annotations.NotNull;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlInsert {
  @NotNull
  @CheckReturnValue
  SqlInsert argInteger(Integer arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argInteger(@NotNull String argName, Integer arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argLong(Long arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argLong(@NotNull String argName, Long arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argFloat(Float arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argFloat(@NotNull String argName, Float arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argDouble(Double arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argDouble(@NotNull String argName, Double arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argBigDecimal(BigDecimal arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argBigDecimal(@NotNull String argName, BigDecimal arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argString(String arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argString(@NotNull String argName, String arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argDate(Date arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argDate(@NotNull String argName, Date arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argDateNowPerApp();

  @NotNull
  @CheckReturnValue
  SqlInsert argDateNowPerApp(@NotNull String argName);

  @NotNull
  @CheckReturnValue
  SqlInsert argDateNowPerDb();

  @NotNull
  @CheckReturnValue
  SqlInsert argDateNowPerDb(@NotNull String argName);

  @NotNull
  @CheckReturnValue
  SqlInsert argBlobBytes(byte[] arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argBlobBytes(@NotNull String argName, byte[] arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argBlobStream(InputStream arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argBlobStream(@NotNull String argName, InputStream arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argClobString(String arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argClobString(@NotNull String argName, String arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argClobReader(Reader arg);

  @NotNull
  @CheckReturnValue
  SqlInsert argClobReader(@NotNull String argName, Reader arg);

  interface Apply {
    void apply(SqlInsert insert);
  }

  @NotNull
  @CheckReturnValue
  SqlInsert withArgs(SqlArgs args);

  @NotNull
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

  @NotNull
  @CheckReturnValue
  SqlInsert argPkSeq(@NotNull String sequenceName);

  /**
   * Use this method to populate the primary key value (assumed to be type Long)
   * from a sequence in the database. This can be used standalone, but is intended
   * to be used in conjunction with insertReturningPkSeq() to both insert and obtain
   * the inserted value in an optimized way (if possible). For databases that are
   * unable to return the value from the insert (such as Derby) this will be simulated
   * first issuing a select to read the sequence, then an insert.
   */
  @NotNull
  @CheckReturnValue
  SqlInsert argPkSeq(@NotNull String argName, @NotNull String sequenceName);

  @NotNull
  @CheckReturnValue
  SqlInsert argPkLong(Long pkValue);

  @NotNull
  @CheckReturnValue
  SqlInsert argPkLong(String argName, Long pkValue);
}
