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
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
  private final DatabaseMock mock;
  private final StatementAdaptor adaptor;
  private final String sql;
  private final Options options;
  private List<Object> parameterList;       // !null ==> traditional ? args
  private Map<String, Object> parameterMap; // !null ==> named :abc args

  public SqlUpdateImpl(Connection connection, DatabaseMock mock, @Nonnull String sql, Options options) {
    this.connection = connection;
    this.mock = mock;
    this.sql = sql;
    this.options = options;
    adaptor = new StatementAdaptor(options);
  }

  @Nonnull
  @Override
  public SqlUpdate argBoolean(Boolean arg) {
    return positionalArg(adaptor.nullString(booleanToString(arg)));
  }

  @Nonnull
  @Override
  public SqlUpdate argBoolean(@Nonnull String argName, Boolean arg) {
    return namedArg(argName, adaptor.nullString(booleanToString(arg)));
  }

  @Override
  @Nonnull
  public SqlUpdate argInteger(@Nullable Integer arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argInteger(@Nonnull String argName, @Nullable Integer arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argLong(@Nullable Long arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argLong(@Nonnull String argName, @Nullable Long arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argFloat(@Nullable Float arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argFloat(@Nonnull String argName, @Nullable Float arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argDouble(@Nullable Double arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argDouble(@Nonnull String argName, @Nullable Double arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argBigDecimal(@Nullable BigDecimal arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argBigDecimal(@Nonnull String argName, @Nullable BigDecimal arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argString(@Nullable String arg) {
    return positionalArg(adaptor.nullString(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argString(@Nonnull String argName, @Nullable String arg) {
    return namedArg(argName, adaptor.nullString(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argDate(@Nullable Date arg) {
    // Date with time
    return positionalArg(adaptor.nullDate(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argDate(@Nonnull String argName, @Nullable Date arg) {
    // Date with time
    return namedArg(argName, adaptor.nullDate(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argLocalDate(@Nullable LocalDate arg) {
    // Date with no time
    return positionalArg(adaptor.nullLocalDate(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argLocalDate(@Nonnull String argName, @Nullable LocalDate arg) {
    // Date with no time
    return namedArg(argName, adaptor.nullLocalDate(arg));
  }

  @Nonnull
  @Override
  public SqlUpdate argDateNowPerApp() {
    return positionalArg(adaptor.nullDate(options.currentDate()));
  }

  @Override
  @Nonnull
  public SqlUpdate argDateNowPerApp(@Nonnull String argName) {
    return namedArg(argName, adaptor.nullDate(options.currentDate()));
  }

  @Nonnull
  @Override
  public SqlUpdate argDateNowPerDb() {
    if (options.useDatePerAppOnly()) {
      return positionalArg(adaptor.nullDate(options.currentDate()));
    }
    return positionalArg(new RewriteArg(options.flavor().dbTimeMillis()));
  }

  @Override
  @Nonnull
  public SqlUpdate argDateNowPerDb(@Nonnull String argName) {
    if (options.useDatePerAppOnly()) {
      return namedArg(argName, adaptor.nullDate(options.currentDate()));
    }
    return namedArg(argName, new RewriteArg(options.flavor().dbTimeMillis()));
  }

  @Override
  @Nonnull
  public SqlUpdate argBlobBytes(@Nullable byte[] arg) {
    return positionalArg(adaptor.nullBytes(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argBlobBytes(@Nonnull String argName, @Nullable byte[] arg) {
    return namedArg(argName, adaptor.nullBytes(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argBlobStream(@Nullable InputStream arg) {
    return positionalArg(adaptor.nullInputStream(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argBlobStream(@Nonnull String argName, @Nullable InputStream arg) {
    return namedArg(argName, adaptor.nullInputStream(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argClobString(@Nullable String arg) {
    return positionalArg(adaptor.nullClobReader(arg == null ? null : new InternalStringReader(arg)));
  }

  @Override
  @Nonnull
  public SqlUpdate argClobString(@Nonnull String argName, @Nullable String arg) {
    return namedArg(argName, adaptor.nullClobReader(arg == null ? null : new InternalStringReader(arg)));
  }

  @Override
  @Nonnull
  public SqlUpdate argClobReader(@Nullable Reader arg) {
    return positionalArg(adaptor.nullClobReader(arg));
  }

  @Override
  @Nonnull
  public SqlUpdate argClobReader(@Nonnull String argName, @Nullable Reader arg) {
    return namedArg(argName, adaptor.nullClobReader(arg));
  }

  @Nonnull
  @Override
  public SqlUpdate withArgs(SqlArgs args) {
    return apply(args);
  }

  @Nonnull
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

      if (connection != null) {
        ps = connection.prepareStatement(executeSql);

        adaptor.addParameters(ps, parameters);
        metric.checkpoint("prep");
        int numAffectedRows = ps.executeUpdate();
        metric.checkpoint("exec", numAffectedRows);
        if (expectedNumAffectedRows > 0 && numAffectedRows != expectedNumAffectedRows) {
          errorCode = options.generateErrorCode();
          throw new WrongNumberOfRowsException("The number of affected rows was " + numAffectedRows + ", but "
              + expectedNumAffectedRows + " were expected." + "\n"
              + DebugSql.exceptionMessage(executeSql, parameters, errorCode, options));
        }
        isSuccess = true;
        return numAffectedRows;
      } else {
        int numAffectedRows = mock.update(executeSql, DebugSql.printDebugOnlySqlString(executeSql, parameters, options));
        metric.checkpoint("stub", numAffectedRows);
        isSuccess = true;
        return numAffectedRows;
      }
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw DatabaseException.wrap(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
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

  @Nonnull
  private SqlUpdate positionalArg(@Nullable Object arg) {
    if (parameterList == null) {
      parameterList = new ArrayList<>();
    }
    parameterList.add(arg);
    return this;
  }

  @Nonnull
  private SqlUpdate namedArg(@Nonnull String argName, @Nullable Object arg) {
    if (parameterMap == null) {
      parameterMap = new HashMap<>();
    }
    if (argName.startsWith(":")) {
      argName = argName.substring(1);
    }
    parameterMap.put(argName, arg);
    return this;
  }

  private String booleanToString(Boolean b) {
    return b == null ? null : b ? "Y" : "N";
  }
}
