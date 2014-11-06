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
  Long queryLongOrNull();

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

  @NotNull
  @CheckReturnValue
  List<Integer> queryIntegers();

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

  <T> T query(RowsHandler<T> rowsHandler);
}
