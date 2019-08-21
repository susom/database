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
import javax.annotation.Nullable;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlUpdate {
  @Nonnull
  @CheckReturnValue
  SqlUpdate argBoolean(Boolean arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argBoolean(@Nonnull String argName, Boolean arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argInteger(@Nullable Integer arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argInteger(@Nonnull String argName, @Nullable Integer arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argLong(@Nullable Long arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argLong(@Nonnull String argName, @Nullable Long arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argFloat(@Nullable Float arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argFloat(@Nonnull String argName, @Nullable Float arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDouble(@Nullable Double arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDouble(@Nonnull String argName, @Nullable Double arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argBigDecimal(@Nullable BigDecimal arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argBigDecimal(@Nonnull String argName, @Nullable BigDecimal arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argString(@Nullable String arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argString(@Nonnull String argName, @Nullable String arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDate(@Nullable Date arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDate(@Nonnull String argName, @Nullable Date arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDbDate(@Nullable LocalDate arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDbDate(@Nonnull String argName, @Nullable LocalDate arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDateNowPerApp();

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDateNowPerApp(@Nonnull String argName);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDateNowPerDb();

  @Nonnull
  @CheckReturnValue
  SqlUpdate argDateNowPerDb(@Nonnull String argName);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argBlobBytes(@Nullable byte[] arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argBlobBytes(@Nonnull String argName, @Nullable byte[] arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argBlobStream(@Nullable InputStream arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argBlobStream(@Nonnull String argName, @Nullable InputStream arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argClobString(@Nullable String arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argClobString(@Nonnull String argName, @Nullable String arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argClobReader(@Nullable Reader arg);

  @Nonnull
  @CheckReturnValue
  SqlUpdate argClobReader(@Nonnull String argName, @Nullable Reader arg);

  /**
   * Call this between setting rows of parameters for a SQL statement. You may call it before
   * setting any parameters, after setting all, or multiple times between rows.
   */
//  SqlUpdate batch();

//  SqlUpdate withTimeoutSeconds(int seconds);

  interface Apply {
    void apply(SqlUpdate update);
  }

  @Nonnull
  @CheckReturnValue
  SqlUpdate withArgs(SqlArgs args);

  @Nonnull
  @CheckReturnValue
  SqlUpdate apply(Apply apply);

  /**
   * Execute the SQL update and return the number of rows was affected.
   */
  int update();

  /**
   * Execute the SQL update and check that the expected number of rows was affected.
   *
   * @throws WrongNumberOfRowsException if the number of rows affected did not match
   *         the value provided
   */
  void update(int expectedRowsUpdated);
}
