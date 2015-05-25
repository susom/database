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

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import javax.annotation.CheckReturnValue;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlSelect {
  @NotNull
  @CheckReturnValue
  SqlSelect argBoolean(Boolean arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argBoolean(@NotNull String argName, Boolean arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argInteger(Integer arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argInteger(@NotNull String argName, Integer arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argLong(Long arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argLong(@NotNull String argName, Long arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argFloat(Float arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argFloat(@NotNull String argName, Float arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argDouble(Double arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argDouble(@NotNull String argName, Double arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argBigDecimal(BigDecimal arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argBigDecimal(@NotNull String argName, BigDecimal arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argString(String arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argString(@NotNull String argName, String arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argDate(Date arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argDate(@NotNull String argName, Date arg);

  @NotNull
  @CheckReturnValue
  SqlSelect argDateNowPerApp();

  @NotNull
  @CheckReturnValue
  SqlSelect argDateNowPerApp(@NotNull String argName);

  @NotNull
  @CheckReturnValue
  SqlSelect argDateNowPerDb();

  @NotNull
  @CheckReturnValue
  SqlSelect argDateNowPerDb(@NotNull String argName);

  @NotNull
  @CheckReturnValue
  SqlSelect withTimeoutSeconds(int seconds);

  @NotNull
  @CheckReturnValue
  SqlSelect withMaxRows(int rows);

  interface Apply {
    void apply(SqlSelect select);
  }

  @NotNull
  @CheckReturnValue
  SqlSelect withArgs(SqlArgs args);

  @NotNull
  @CheckReturnValue
  SqlSelect apply(Apply apply);

  @Nullable
  @CheckReturnValue
  Boolean queryBooleanOrNull();

  @CheckReturnValue
  boolean queryBooleanOrFalse();

  @CheckReturnValue
  boolean queryBooleanOrTrue();

  @Nullable
  @CheckReturnValue
  Long queryLongOrNull();

  @CheckReturnValue
  long queryLongOrZero();

  /**
   * Shorthand for reading numbers from the first column of the result.
   *
   * @return the first column values, omitting any that were null
   */
  @NotNull
  @CheckReturnValue
  List<Long> queryLongs();

  @Nullable
  @CheckReturnValue
  Integer queryIntegerOrNull();

  @CheckReturnValue
  int queryIntegerOrZero();

  @NotNull
  @CheckReturnValue
  List<Integer> queryIntegers();

  @Nullable
  @CheckReturnValue
  Float queryFloatOrNull();

  @CheckReturnValue
  float queryFloatOrZero();

  @NotNull
  @CheckReturnValue
  List<Float> queryFloats();

  @Nullable
  @CheckReturnValue
  Double queryDoubleOrNull();

  @CheckReturnValue
  double queryDoubleOrZero();

  @NotNull
  @CheckReturnValue
  List<Double> queryDoubles();

  @Nullable
  @CheckReturnValue
  BigDecimal queryBigDecimalOrNull();

  @NotNull
  @CheckReturnValue
  BigDecimal queryBigDecimalOrZero();

  @NotNull
  @CheckReturnValue
  List<BigDecimal> queryBigDecimals();

  @Nullable
  @CheckReturnValue
  String queryStringOrNull();

  @NotNull
  @CheckReturnValue
  String queryStringOrEmpty();

  /**
   * Shorthand for reading strings from the first column of the result.
   *
   * @return the first column values, omitting any that were null
   */
  @NotNull
  @CheckReturnValue
  List<String> queryStrings();

  @Nullable
  @CheckReturnValue
  Date queryDateOrNull();

  @NotNull
  @CheckReturnValue
  List<Date> queryDates();

  /**
   * This is the most generic and low-level way to iterate the query results.
   * Consider using one of the other methods that can handle the iteration for you.
   *
   * @param rowsHandler the process() method of this handler will be called once
   *                    and it will be responsible for iterating the results
   */
  <T> T query(RowsHandler<T> rowsHandler);

  /**
   * Query zero or one row. If zero rows are available a null will be returned.
   * If more than one row is available a {@link ConstraintViolationException}
   * will be thrown.
   *
   * @param rowHandler the process() method of this handler will be called once
   *                   if there are results, or will not be called if there are
   *                   no results
   */
  <T> T queryOneOrNull(RowHandler<T> rowHandler);

  /**
   * Query exactly one row. If zero rows are available or more than one row is
   * available a {@link ConstraintViolationException} will be thrown.
   *
   * @param rowHandler the process() method of this handler will be called once
   *                   if there are results, or will not be called if there are
   *                   no results
   */
  <T> T queryOneOrThrow(RowHandler<T> rowHandler);

  /**
   * Query zero or one row. If zero rows are available a null will be returned.
   * If more than one row is available the first row will be returned.
   *
   * @param rowHandler the process() method of this handler will be called once
   *                   if there are results (for the first row), or will not be
   *                   called if there are no results
   */
  <T> T queryFirstOrNull(RowHandler<T> rowHandler);

  /**
   * Query zero or one row. If zero rows are available a {@link ConstraintViolationException}
   * will be thrown. If more than one row is available the first row will be returned.
   *
   * @param rowHandler the process() method of this handler will be called once
   *                   if there are results (for the first row), or will not be
   *                   called if there are no results
   */
  <T> T queryFirstOrThrow(RowHandler<T> rowHandler);

  /**
   * Query zero or more rows. If zero rows are available an empty list will be returned.
   * If one or more rows are available each row will be read and added to a list, which
   * is returned.
   *
   * @param rowHandler the process() method of this handler will be called once
   *                   for each row in the result, or will not be called if there are
   *                   no results. Only non-null values returned will be added to the
   *                   result list.
   */
  <T> List<T> queryMany(RowHandler<T> rowHandler);
}
