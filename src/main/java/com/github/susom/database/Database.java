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

import javax.inject.Provider;

import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

/**
 * Primary class for accessing a relational (SQL) database.
 *
 * @author garricko
 */
public interface Database extends Provider<Database> {
  @NotNull
  SqlInsert insert(@Language("SQL") String sql);

  @NotNull
  SqlSelect select(@Language("SQL") String sql);

  @NotNull
  SqlUpdate update(@Language("SQL") String sql);

  @NotNull
  SqlUpdate delete(@Language("SQL") String sql);

  @NotNull
  Ddl ddl(@Language("SQL") String sql);

  void commitNow();

  void rollbackNow();

  @NotNull
  Flavor flavor();

  /**
   * Convenience method to deal with mutually incompatible syntax for this. For example:
   *
   * <p>Oracle: 'drop sequence x'</p>
   * <p>Derby: 'drop sequence x restrict'</p>"
   */
  void dropSequenceQuietly(String sequenceName);

  void dropTableQuietly(String tableName);
}
