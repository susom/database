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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlUpdate {
  @NotNull
  SqlUpdate argInteger(@Nullable Integer arg);

  @NotNull
  SqlUpdate argInteger(@NotNull String argName, @Nullable Integer arg);

  @NotNull
  SqlUpdate argLong(@Nullable Long arg);

  @NotNull
  SqlUpdate argLong(@NotNull String argName, @Nullable Long arg);

  @NotNull
  SqlUpdate argFloat(@Nullable Float arg);

  @NotNull
  SqlUpdate argFloat(@NotNull String argName, @Nullable Float arg);

  @NotNull
  SqlUpdate argDouble(@Nullable Double arg);

  @NotNull
  SqlUpdate argDouble(@NotNull String argName, @Nullable Double arg);

  @NotNull
  SqlUpdate argBigDecimal(@Nullable BigDecimal arg);

  @NotNull
  SqlUpdate argBigDecimal(@NotNull String argName, @Nullable BigDecimal arg);

  @NotNull
  SqlUpdate argString(@Nullable String arg);

  @NotNull
  SqlUpdate argString(@NotNull String argName, @Nullable String arg);

  @NotNull
  SqlUpdate argDate(@Nullable Date arg);

  @NotNull
  SqlUpdate argDate(@NotNull String argName, @Nullable Date arg);

  @NotNull
  SqlUpdate argDateNowPerApp(@NotNull String argName);

  @NotNull
  SqlUpdate argDateNowPerDb(@NotNull String argName);

  @NotNull
  SqlUpdate argBlobBytes(@Nullable byte[] arg);

  @NotNull
  SqlUpdate argBlobBytes(@NotNull String argName, @Nullable byte[] arg);

  @NotNull
  SqlUpdate argBlobStream(@Nullable InputStream arg);

  @NotNull
  SqlUpdate argBlobStream(@NotNull String argName, @Nullable InputStream arg);

  @NotNull
  SqlUpdate argClobString(@Nullable String arg);

  @NotNull
  SqlUpdate argClobString(@NotNull String argName, @Nullable String arg);

  @NotNull
  SqlUpdate argClobReader(@Nullable Reader arg);

  @NotNull
  SqlUpdate argClobReader(@NotNull String argName, @Nullable Reader arg);

  /**
   * Call this between setting rows of parameters for a SQL statement. You may call it before
   * setting any parameters, after setting all, or multiple times between rows.
   */
//  SqlUpdate batch();

//  SqlUpdate withTimeoutSeconds(int seconds);

  interface Apply {
    void apply(SqlUpdate update);
  }

  @NotNull
  SqlUpdate apply(Apply apply);

  int update();

  /**
   * Execute the SQL update and check that the expected number of rows was updated. A
   * DatabaseException will be thrown
   */
  void update(int expectedRowsUpdated);
}
