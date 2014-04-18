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

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlSelect {
  SqlSelect argInteger(Integer arg);

  SqlSelect argInteger(String argName, Integer arg);

  SqlSelect argLong(Long arg);

  SqlSelect argLong(String argName, Long arg);

  SqlSelect argFloat(Float arg);

  SqlSelect argFloat(String argName, Float arg);

  SqlSelect argDouble(Double arg);

  SqlSelect argDouble(String argName, Double arg);

  SqlSelect argBigDecimal(BigDecimal arg);

  SqlSelect argBigDecimal(String argName, BigDecimal arg);

  SqlSelect argString(String arg);

  SqlSelect argString(String argName, String arg);

  SqlSelect argDate(Date arg);

  SqlSelect argDate(String argName, Date arg);

  SqlSelect withTimeoutSeconds(int seconds);

  SqlSelect withMaxRows(int rows);

  Long queryLong();

  List<Long> queryLongs();

  <T> T query(RowsHandler<T> rowsHandler);
}
