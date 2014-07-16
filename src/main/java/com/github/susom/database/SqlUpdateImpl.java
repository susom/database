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

import com.github.susom.database.MixedParameterSql.RewriteArg;

/**
 * This is the key class for configuring (query parameters) and executing a database query.
 *
 * @author garricko
 */
public class SqlUpdateImpl implements SqlUpdate {
  private static final Logger log = LoggerFactory.getLogger(Database.class);
  private final Connection connection;
  private final StatementAdaptor adaptor;
  private final String sql;
  private final Options options;
  private List<Object> parameterList;       // !null ==> traditional ? args
  private Map<String, Object> parameterMap; // !null ==> named :abc args

  public SqlUpdateImpl(@NotNull Connection connection, @NotNull String sql, Options options) {
    this.connection = connection;
    this.sql = sql;
    this.options = options;
    adaptor = new StatementAdaptor(options);
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

  @NotNull
  @Override
  public SqlUpdate argDateNowPerApp() {
    return positionalArg(adaptor.nullDate(options.currentDate()));
  }

  @Override
  @NotNull
  public SqlUpdate argDateNowPerApp(@NotNull String argName) {
    return namedArg(argName, adaptor.nullDate(options.currentDate()));
  }

  @NotNull
  @Override
  public SqlUpdate argDateNowPerDb() {
    return positionalArg(new RewriteArg(options.flavor().sysdate()));
  }

  @Override
  @NotNull
  public SqlUpdate argDateNowPerDb(@NotNull String argName) {
    return namedArg(argName, new RewriteArg(options.flavor().sysdate()));
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
  public SqlUpdate argBlobStream(@Nullable InputStream arg) {
    return positionalArg(adaptor.nullInputStream(arg));
  }

  @Override
  @NotNull
  public SqlUpdate argBlobStream(@NotNull String argName, @Nullable InputStream arg) {
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

  @NotNull
  @Override
  public SqlUpdate apply(Apply apply) {
    apply.apply(this);
    return this;
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

    String executeSql = sql;
    Object[] parameters = null;

    boolean isSuccess = false;
    String errorCode = null;
    Exception logEx = null;
    try {
      MixedParameterSql mpSql = new MixedParameterSql(sql, parameterList, parameterMap);
      executeSql = mpSql.getSqlToExecute();
      parameters = mpSql.getArgs();

      ps = connection.prepareStatement(executeSql);

      adaptor.addParameters(ps, parameters);
      metric.checkpoint("prep");
      int numAffectedRows = ps.executeUpdate();
      metric.checkpoint("exec[" + numAffectedRows + "]");
      if (expectedNumAffectedRows > 0 && numAffectedRows != expectedNumAffectedRows) {
        errorCode = options.generateErrorCode();
        throw new WrongNumberOfRowsException("The number of affected rows was " + numAffectedRows + ", but "
            + expectedNumAffectedRows + " were expected." + "\n"
            + DebugSql.exceptionMessage(executeSql, parameters, errorCode, options));
      }
      isSuccess = true;
      return numAffectedRows;
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw new DatabaseException(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Update", log, metric, executeSql, parameters, options);
      } else {
        DebugSql.logError("Update", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }

  @NotNull
  private SqlUpdate positionalArg(@Nullable Object arg) {
    if (parameterList == null) {
      parameterList = new ArrayList<>();
    }
    parameterList.add(arg);
    return this;
  }

  @NotNull
  private SqlUpdate namedArg(@NotNull String argName, @Nullable Object arg) {
    if (parameterMap == null) {
      parameterMap = new HashMap<>();
    }
    if (argName.startsWith(":")) {
      argName = argName.substring(1);
    }
    parameterMap.put(argName, arg);
    return this;
  }
}
