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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlSelect {
  @NotNull
  SqlSelect argInteger(Integer arg);

  @NotNull
  SqlSelect argInteger(@NotNull String argName, Integer arg);

  @NotNull
  SqlSelect argLong(Long arg);

  @NotNull
  SqlSelect argLong(@NotNull String argName, Long arg);

  @NotNull
  SqlSelect argFloat(Float arg);

  @NotNull
  SqlSelect argFloat(@NotNull String argName, Float arg);

  @NotNull
  SqlSelect argDouble(Double arg);

  @NotNull
  SqlSelect argDouble(@NotNull String argName, Double arg);

  @NotNull
  SqlSelect argBigDecimal(BigDecimal arg);

  @NotNull
  SqlSelect argBigDecimal(@NotNull String argName, BigDecimal arg);

  @NotNull
  SqlSelect argString(String arg);

  @NotNull
  SqlSelect argString(@NotNull String argName, String arg);

  @NotNull
  SqlSelect argDate(Date arg);

  @NotNull
  SqlSelect argDate(@NotNull String argName, Date arg);

  @NotNull
  SqlSelect argDateNowPerApp();

  @NotNull
  SqlSelect argDateNowPerApp(@NotNull String argName);

  @NotNull
  SqlSelect argDateNowPerDb();

  @NotNull
  SqlSelect argDateNowPerDb(@NotNull String argName);

  @NotNull
  SqlSelect withTimeoutSeconds(int seconds);

  @NotNull
  SqlSelect withMaxRows(int rows);

  interface Apply {
    void apply(SqlSelect select);
  }

  @NotNull
  SqlSelect apply(Apply apply);

  @Nullable
  Long queryLongOrNull();

  /**
   * Shorthand for reading numbers from the first column of the result.
   *
   * @return the first column values, omitting any that were null
   */
  @NotNull
  List<Long> queryLongs();

  @Nullable
  Integer queryIntegerOrNull();

  @NotNull
  List<Integer> queryIntegers();

  @Nullable
  String queryStringOrNull();

  @NotNull
  String queryStringOrEmpty();

  /**
   * Shorthand for reading strings from the first column of the result.
   *
   * @return the first column values, omitting any that were null
   */
  @NotNull
  List<String> queryStrings();

  @Nullable
  Date queryDateOrNull();

  @NotNull
  List<Date> queryDates();

  <T> T query(RowsHandler<T> rowsHandler);
}
