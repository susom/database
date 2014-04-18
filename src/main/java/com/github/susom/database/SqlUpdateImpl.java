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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the key class for configuring (query parameters) and executing a database query.
 *
 * @author garricko
 */
public class SqlUpdateImpl implements SqlUpdate {
  private static final Logger log = LoggerFactory.getLogger(SqlUpdateImpl.class);
  private static final Object[] ZERO_LENGTH_OBJECT_ARRAY = new Object[0];
  private final Connection connection;
  private final StatementAdaptor adaptor;
  private final String sql;
  private List<Object> parameterList;       // !null ==> traditional ? args
  private Map<String, Object> parameterMap; // !null ==> named :abc args

  public SqlUpdateImpl(@NotNull Connection connection, @NotNull String sql) {
    this.connection = connection;
    this.sql = sql;
    adaptor = new StatementAdaptor();
  }

  @Override
  @NotNull
  public SqlUpdate argInteger(@Nullable Integer arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argInteger(@NotNull String argName, @Nullable Integer arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argLong(@Nullable Long arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argLong(@NotNull String argName, @Nullable Long arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argFloat(@Nullable Float arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argFloat(@NotNull String argName, @Nullable Float arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argDouble(@Nullable Double arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argDouble(@NotNull String argName, @Nullable Double arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argBigDecimal(@Nullable BigDecimal arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argBigDecimal(@NotNull String argName, @Nullable BigDecimal arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argString(@Nullable String arg) {
    return positionalArg(adaptor.nullString(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argString(@NotNull String argName, @Nullable String arg) {
    return namedArg(argName, adaptor.nullString(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argDate(@Nullable Date arg) {
    return positionalArg(adaptor.nullDate(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argDate(@NotNull String argName, @Nullable Date arg) {
    return namedArg(argName, adaptor.nullDate(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argBlobBytes(@Nullable byte[] arg) {
    return positionalArg(adaptor.nullBytes(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argBlobBytes(@NotNull String argName, @Nullable byte[] arg) {
    return namedArg(argName, adaptor.nullBytes(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argBlobInputStream(@Nullable InputStream arg) {
    return positionalArg(adaptor.nullInputStream(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argBlobInputStream(@NotNull String argName, @Nullable InputStream arg) {
    return namedArg(argName, adaptor.nullInputStream(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argClobString(@Nullable String arg) {
    return positionalArg(adaptor.nullString(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argClobString(@NotNull String argName, @Nullable String arg) {
    return namedArg(argName, adaptor.nullString(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argClobReader(@Nullable Reader arg) {
    return positionalArg(adaptor.nullClobReader(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argClobReader(@NotNull String argName, @Nullable Reader arg) {
    return namedArg(argName, adaptor.nullClobReader(arg));
  }

  @Override
  public int update() {
    return updateInternal(0);
  }

  @Override
  public void update(int expectedNumAffectedRows) {
    updateInternal(expectedNumAffectedRows);
  }

  private int updateInternal(int expectedNumAffectedRows) {
    PreparedStatement ps = null;
    Metric metric = new Metric(log.isDebugEnabled());

    String executeSql;
    Object[] parameters = ZERO_LENGTH_OBJECT_ARRAY;
    if (parameterMap != null && parameterMap.size() > 0) {
      NamedParameterSql paramSql = new NamedParameterSql(sql);
      executeSql = paramSql.getSqlToExecute();
      parameters = paramSql.toArgs(parameterMap);
    } else {
      executeSql = sql;
      if (parameterList != null) {
        parameters = parameterList.toArray(new Object[parameterList.size()]);
      }
    }

    try {
      ps = connection.prepareStatement(executeSql);

      adaptor.addParameters(ps, parameters);
      metric.checkpoint("prep");
      int numAffectedRows = ps.executeUpdate();
      metric.checkpoint("exec");
      if (expectedNumAffectedRows > 0 && numAffectedRows != expectedNumAffectedRows) {
        throw new WrongNumberOfRowsException("The number of affected rows was " + numAffectedRows + ", but "
            + expectedNumAffectedRows + " were expected." + "\n" + toMessage(executeSql, parameters));
      }
      return numAffectedRows;
    } catch (Exception e) {
      throw new DatabaseException(toMessage(executeSql, parameters), e);
    } finally {
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (log.isDebugEnabled()) {
        log.debug("Update: " + metric.getMessage() + " " + new DebugSql(executeSql, parameters));
      }
    }
  }

  @NotNull
  private SqlUpdate positionalArg(@Nullable Object arg) {
    if (parameterMap != null) {
      throw new DatabaseException("Use either positional or named query parameters, not both");
    }
    if (parameterList == null) {
      parameterList = new ArrayList<>();
    }
    parameterList.add(arg);
    return this;
  }

  @NotNull
  private SqlUpdate namedArg(@NotNull String argName, @Nullable Object arg) {
    if (parameterList != null) {
      throw new DatabaseException("Use either positional or named query parameters, not both");
    }
    if (parameterMap == null) {
      parameterMap = new HashMap<>();
    }
    if (argName.startsWith(":")) {
      argName = argName.substring(1);
    }
    parameterMap.put(argName, arg);
    return this;
  }

  private String toMessage(String sql, Object[] parameters) {
    return "Error executing SQL: " + new DebugSql(sql, parameters);
  }
}
