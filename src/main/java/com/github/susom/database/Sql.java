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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Stack;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
/*>>>
import org.checkerframework.checker.tainting.qual.Untainted;
*/

/**
 * This class is useful for dynamically generating SQL. It can "buffer" the
 * various arg*() calls and replay them later via the apply(sqlArgs) methods.
 *
 * @author garricko
 */
/*@Untainted*/
public class Sql implements SqlInsert.Apply, SqlUpdate.Apply, SqlSelect.Apply {
  private StringBuilder sql = new StringBuilder();
  private Stack<Boolean> listFirstItem = new Stack<>();
  private List<SqlArgs> batched;
  private SqlArgs sqlArgs = new SqlArgs();

  public Sql() {
    // Nothing to do
  }

  public Sql(/*@Untainted*/ String sql) {
    this.sql.append(sql);
  }

  public static Sql insert(/*@Untainted*/ String table, SqlArgs args) {
    return insert(table, Collections.singletonList(args));
  }

  public static Sql insert(/*@Untainted*/ String table, List<SqlArgs> args) {
    Sql sql = null;
    List<String> expectedColumns = null;

    for (SqlArgs arg : args) {
      if (arg.positionalCount() > 0) {
        throw new DatabaseException("The SqlArgs must all be named to do this");
      }
      List<String> columns = arg.names();
      if (columns.size() < 1) {
        throw new DatabaseException("You must add named arguments to SqlArgs");
      }
      if (sql == null) {
        expectedColumns = columns;
        sql = new Sql("insert into ").append(table);
        sql.listStart(" (");
        for (String column : columns) {
          sql.listSeparator(",");
          sql.append(column);
        }
        sql.listEnd(") values (");
        sql.appendQuestionMarks(columns.size());
        sql.append(")");
      } else {
        if (!expectedColumns.equals(columns)) {
          throw new DatabaseException("The columns for all rows in a batch must match. \nFirst: " + expectedColumns
              + "\nCurrent: " + columns);
        }
        sql.batch();
      }
      sql.setSqlArgs(arg.makePositional());
    }
    return sql;
  }

  public Sql setSqlArgs(SqlArgs args) {
    sqlArgs = args;
    return this;
  }

  public Sql batch() {
    if (sqlArgs.argCount() > 0) {
      if (batched == null) {
        batched = new ArrayList<>();
      }
      batched.add(sqlArgs);
      sqlArgs = new SqlArgs();
    }
    return this;
  }

  public int argCount() {
    return sqlArgs.argCount();
  }

  public Sql appendQuestionMarks(int howMany) {
    boolean first = true;
    for (int i = 0; i < howMany; i++) {
      if (first) {
        first = false;
        append("?");
      } else {
        append(",?");
      }
    }
    return this;
  }

  public Sql append(/*@Untainted*/ String sql) {
    this.sql.append(sql);
    return this;
  }

  public Sql append(boolean value) {
    this.sql.append(value);
    return this;
  }

  public Sql append(int value) {
    this.sql.append(value);
    return this;
  }

  public Sql append(long value) {
    this.sql.append(value);
    return this;
  }

  public Sql append(float value) {
    this.sql.append(value);
    return this;
  }

  public Sql append(double value) {
    this.sql.append(value);
    return this;
  }

  public Sql deleteCharAt(int index) {
    this.sql.deleteCharAt(index);
    return this;
  }

  public Sql replace(int start, int end, /*@Untainted*/ String str) {
    this.sql.replace(start, end, str);
    return this;
  }

  public Sql insert(int offset, /*@Untainted*/ String str) {
    this.sql.insert(offset, str);
    return this;
  }

  public Sql insert(int offset, boolean value) {
    this.sql.insert(offset, value);
    return this;
  }

  public Sql insert(int offset, int value) {
    this.sql.insert(offset, value);
    return this;
  }

  public Sql insert(int offset, long value) {
    this.sql.insert(offset, value);
    return this;
  }

  public Sql insert(int offset, double value) {
    this.sql.insert(offset, value);
    return this;
  }

  public Sql insert(int offset, float value) {
    this.sql.insert(offset, value);
    return this;
  }

  public int indexOf(String str) {
    return this.sql.indexOf(str);
  }

  public int indexOf(String str, int fromIndex) {
    return this.sql.indexOf(str, fromIndex);
  }

  public int lastIndexOf(String str) {
    return this.sql.lastIndexOf(str);
  }

  public int lastIndexOf(String str, int fromIndex) {
    return this.sql.lastIndexOf(str, fromIndex);
  }

  /**
   * Appends the bit of sql and notes that a list, or a sublist, has started.
   *
   * Each list started must have be ended. "Lists" are only to support using listSeparator(sep)
   */
  public Sql listStart(/*@Untainted*/ String sql) {
    listFirstItem.push(true);
    return append(sql);
  }

  /**
   * Appends the passed bit of sql only if a previous item has already been appended,
   * and notes that the list is not empty.
   */
  public Sql listSeparator(/*@Untainted*/ String sql) {
    if (listFirstItem.peek()) {
      listFirstItem.pop();
      listFirstItem.push(false);
      return this;
    } else {
      return append(sql);
    }
  }


  public Sql listEnd(/*@Untainted*/ String sql) {
    listFirstItem.pop();
    return append(sql);
  }

  /*@Untainted*/
  public String sql() {
    return sql.toString();
  }

  /**
   * Same as sql(), provided for drop-in compatibility with StringBuilder.
   */
  /*@Untainted*/
  public String toString() {
    return sql();
  }

  public static enum ColumnType {
    Integer, Long, Float, Double, BigDecimal, String, ClobString, ClobStream,
    BlobBytes, BlobStream, Date, DateNowPerApp, DateNowPerDb, Boolean
  }

  private static class Invocation {
    ColumnType columnType;
    String argName;
    Object arg;

    Invocation(ColumnType columnType, String argName, Object arg) {
      this.columnType = columnType;
      this.argName = argName;
      this.arg = arg;
    }
  }

  @Nonnull
  public Sql argBoolean(@Nullable Boolean arg) {
    sqlArgs.argBoolean(arg);
    return this;
  }

  @Nonnull
  public Sql argBoolean(@Nonnull String argName, @Nullable Boolean arg) {
    sqlArgs.argBoolean(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argInteger(@Nullable Integer arg) {
    sqlArgs.argInteger(arg);
    return this;
  }

  @Nonnull
  public Sql argInteger(@Nonnull String argName, @Nullable Integer arg) {
    sqlArgs.argInteger(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argLong(@Nullable Long arg) {
    sqlArgs.argLong(arg);
    return this;
  }

  @Nonnull
  public Sql argLong(@Nonnull String argName, @Nullable Long arg) {
    sqlArgs.argLong(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argFloat(@Nullable Float arg) {
    sqlArgs.argFloat(arg);
    return this;
  }

  @Nonnull
  public Sql argFloat(@Nonnull String argName, @Nullable Float arg) {
    sqlArgs.argFloat(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argDouble(@Nullable Double arg) {
    sqlArgs.argDouble(arg);
    return this;
  }

  @Nonnull
  public Sql argDouble(@Nonnull String argName, @Nullable Double arg) {
    sqlArgs.argDouble(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argBigDecimal(@Nullable BigDecimal arg) {
    sqlArgs.argBigDecimal(arg);
    return this;
  }

  @Nonnull
  public Sql argBigDecimal(@Nonnull String argName, @Nullable BigDecimal arg) {
    sqlArgs.argBigDecimal(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argString(@Nullable String arg) {
    sqlArgs.argString(arg);
    return this;
  }

  @Nonnull
  public Sql argString(@Nonnull String argName, @Nullable String arg) {
    sqlArgs.argString(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argDate(@Nullable Date arg) {
    sqlArgs.argDate(arg);
    return this;
  }

  @Nonnull
  public Sql argDate(@Nonnull String argName, @Nullable Date arg) {
    sqlArgs.argDate(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argDateNowPerApp() {
    sqlArgs.argDateNowPerApp();
    return this;
  }

  @Nonnull
  public Sql argDateNowPerApp(@Nonnull String argName) {
    sqlArgs.argDateNowPerApp(argName);
    return this;
  }

  @Nonnull
  public Sql argDateNowPerDb() {
    sqlArgs.argDateNowPerDb();
    return this;
  }

  @Nonnull
  public Sql argDateNowPerDb(@Nonnull String argName) {
    sqlArgs.argDateNowPerDb(argName);
    return this;
  }

  @Nonnull
  public Sql argBlobBytes(@Nullable byte[] arg) {
    sqlArgs.argBlobBytes(arg);
    return this;
  }

  @Nonnull
  public Sql argBlobBytes(@Nonnull String argName, @Nullable byte[] arg) {
    sqlArgs.argBlobBytes(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argBlobInputStream(@Nullable InputStream arg) {
    sqlArgs.argBlobInputStream(arg);
    return this;
  }

  @Nonnull
  public Sql argBlobInputStream(@Nonnull String argName, @Nullable InputStream arg) {
    sqlArgs.argBlobInputStream(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argClobString(@Nullable String arg) {
    sqlArgs.argClobString(arg);
    return this;
  }

  @Nonnull
  public Sql argClobString(@Nonnull String argName, @Nullable String arg) {
    sqlArgs.argClobString(argName, arg);
    return this;
  }

  @Nonnull
  public Sql argClobReader(@Nullable Reader arg) {
    sqlArgs.argClobReader(arg);
    return this;
  }

  @Nonnull
  public Sql argClobReader(@Nonnull String argName, @Nullable Reader arg) {
    sqlArgs.argClobReader(argName, arg);
    return this;
  }

  @Override
  public void apply(SqlSelect select) {
    if (batched != null) {
      throw new DatabaseException("Batch not supported for select");
    }
    sqlArgs.apply(select);
  }

  @Override
  public void apply(SqlInsert insert) {
    if (batched != null) {
      batch();
      for (SqlArgs args : batched) {
        args.apply(insert);
        insert.batch();
      }
    } else {
      sqlArgs.apply(insert);
    }
  }

  @Override
  public void apply(SqlUpdate update) {
    if (batched != null) {
      throw new DatabaseException("Batch not supported for update");
    }

    sqlArgs.apply(update);
  }
}
