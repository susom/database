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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.susom.database.MixedParameterSql.RewriteArg;

/**
 * This is the key class for configuring (query parameters) and executing a database query.
 *
 * @author garricko
 */
public class SqlInsertImpl implements SqlInsert {
  private static final Logger log = LoggerFactory.getLogger(Database.class);
  private final Connection connection;
  private final DatabaseMock mock;
  private final StatementAdaptor adaptor;
  private final String sql;
  private final Options options;
  private List<Object> parameterList;       // !null ==> traditional ? args
  private Map<String, Object> parameterMap; // !null ==> named :abc args
  private String pkArgName;
  private int pkPos;
  private String pkSeqName;
  private Long pkLong;

  public SqlInsertImpl(Connection connection, DatabaseMock mock, String sql, Options options) {
    this.connection = connection;
    this.mock = mock;
    this.sql = sql;
    this.options = options;
    adaptor = new StatementAdaptor(options);
  }

  @NotNull
  @Override
  public SqlInsert argBoolean(Boolean arg) {
    return positionalArg(adaptor.nullString(booleanToString(arg)));
  }

  @NotNull
  @Override
  public SqlInsert argBoolean(@NotNull String argName, Boolean arg) {
    return namedArg(argName, adaptor.nullString(booleanToString(arg)));
  }

  @Override
  @NotNull
  public SqlInsert argInteger(Integer arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argInteger(@NotNull String argName, Integer arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argLong(Long arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argLong(@NotNull String argName, Long arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argFloat(Float arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argFloat(@NotNull String argName, Float arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argDouble(Double arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argDouble(@NotNull String argName, Double arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argBigDecimal(BigDecimal arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argBigDecimal(@NotNull String argName, BigDecimal arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argString(String arg) {
    return positionalArg(adaptor.nullString(arg));
  }

  @Override
  @NotNull
  public SqlInsert argString(@NotNull String argName, String arg) {
    return namedArg(argName, adaptor.nullString(arg));
  }

  @Override
  @NotNull
  public SqlInsert argDate(Date arg) {
    return positionalArg(adaptor.nullDate(arg));
  }

  @Override
  @NotNull
  public SqlInsert argDate(@NotNull String argName, Date arg) {
    return namedArg(argName, adaptor.nullDate(arg));
  }

  @NotNull
  @Override
  public SqlInsert argDateNowPerApp() {
    return positionalArg(adaptor.nullDate(options.currentDate()));
  }

  @Override
  @NotNull
  public SqlInsert argDateNowPerApp(@NotNull String argName) {
    return namedArg(argName, adaptor.nullDate(options.currentDate()));
  }

  @NotNull
  @Override
  public SqlInsert argDateNowPerDb() {
    if (options.useDatePerAppOnly()) {
      return positionalArg(adaptor.nullDate(options.currentDate()));
    }
    return positionalArg(new RewriteArg(options.flavor().dbTimeMillis()));
  }

  @Override
  @NotNull
  public SqlInsert argDateNowPerDb(@NotNull String argName) {
    if (options.useDatePerAppOnly()) {
      return namedArg(argName, adaptor.nullDate(options.currentDate()));
    }
    return namedArg(argName, new RewriteArg(options.flavor().dbTimeMillis()));
  }

  @Override
  @NotNull
  public SqlInsert argBlobBytes(byte[] arg) {
    return positionalArg(adaptor.nullBytes(arg));
  }

  @Override
  @NotNull
  public SqlInsert argBlobBytes(@NotNull String argName, byte[] arg) {
    return namedArg(argName, adaptor.nullBytes(arg));
  }

  @Override
  @NotNull
  public SqlInsert argBlobStream(InputStream arg) {
    return positionalArg(adaptor.nullInputStream(arg));
  }

  @Override
  @NotNull
  public SqlInsert argBlobStream(@NotNull String argName, InputStream arg) {
    return namedArg(argName, adaptor.nullInputStream(arg));
  }

  @Override
  @NotNull
  public SqlInsert argClobString(String arg) {
    return positionalArg(adaptor.nullClobReader(arg == null ? null : new StringReader(arg)));
  }

  @Override
  @NotNull
  public SqlInsert argClobString(@NotNull String argName, String arg) {
    return namedArg(argName, adaptor.nullClobReader(arg == null ? null : new StringReader(arg)));
  }

  @Override
  @NotNull
  public SqlInsert argClobReader(Reader arg) {
    return positionalArg(adaptor.nullClobReader(arg));
  }

  @Override
  @NotNull
  public SqlInsert argClobReader(@NotNull String argName, Reader arg) {
    return namedArg(argName, adaptor.nullClobReader(arg));
  }

  @NotNull
  @Override
  public SqlInsert withArgs(SqlArgs args) {
    return apply(args);
  }

  @NotNull
  @Override
  public SqlInsert apply(Apply apply) {
    apply.apply(this);
    return this;
  }

  @Override
  public int insert() {
    return updateInternal(0);
  }

  @Override
  public void insert(int expectedRowsUpdated) {
    updateInternal(expectedRowsUpdated);
  }

  @Override
  public Long insertReturningPkSeq(String primaryKeyColumnName) {
    if (!hasPk()) {
      throw new DatabaseException("Call argPkSeq() before insertReturningPkSeq()");
    }

    if (options.flavor().supportsInsertReturning()) {
      return updateInternal(1, primaryKeyColumnName);
    } else {
      // Simulate by issuing a select for the next sequence value, inserting, and returning it
      Long pk = new SqlSelectImpl(connection, mock, options.flavor().sequenceSelectNextVal(pkSeqName), options).queryLongOrNull();
      if (pk == null) {
        throw new DatabaseException("Unable to retrieve next sequence value from " + pkSeqName);
      }
      if (pkArgName != null) {
        namedArg(pkArgName, adaptor.nullNumeric(pk));
      } else {
        parameterList.set(pkPos, adaptor.nullNumeric(pk));
      }
      updateInternal(1);
      return pk;
    }
  }

  @Override
  public <T> T insertReturning(String tableName, String primaryKeyColumnName, RowsHandler<T> handler,
                               String... otherColumnNames) {
    if (!hasPk()) {
      throw new DatabaseException("Identify a primary key with argPk*() before insertReturning()");
    }

    if (options.flavor().supportsInsertReturning()) {
      return updateInternal(1, primaryKeyColumnName, handler, otherColumnNames);
    } else if (pkSeqName != null) {
      // Simulate by issuing a select for the next sequence value, inserting, and returning it
      Long pk = new SqlSelectImpl(connection, mock, options.flavor().sequenceSelectNextVal(pkSeqName), options)
          .queryLongOrNull();
      if (pk == null) {
        throw new DatabaseException("Unable to retrieve next sequence value from " + pkSeqName);
      }
      if (pkArgName != null) {
        namedArg(pkArgName, adaptor.nullNumeric(pk));
      } else {
        parameterList.set(pkPos, adaptor.nullNumeric(pk));
      }
      updateInternal(1);
      StringBuilder sql = new StringBuilder();
      sql.append("select ").append(primaryKeyColumnName);
      for (String colName : otherColumnNames) {
        sql.append(", ").append(colName);
      }
      sql.append(" from ").append(tableName).append(" where ").append(primaryKeyColumnName).append("=?");
      return new SqlSelectImpl(connection, mock, sql.toString(), options).argLong(pk).query(handler);
    } else if (pkLong != null) {
      // Insert the value, then do a select based on the primary key
      updateInternal(1);
      StringBuilder sql = new StringBuilder();
      sql.append("select ").append(primaryKeyColumnName);
      for (String colName : otherColumnNames) {
        sql.append(", ").append(colName);
      }
      sql.append(" from ").append(tableName).append(" where ").append(primaryKeyColumnName).append("=?");
      return new SqlSelectImpl(connection, mock, sql.toString(), options).argLong(pkLong).query(handler);
    } else {
      // Should never happen if our safety checks worked
      throw new DatabaseException("Internal error");
    }
  }

  @NotNull
  @Override
  public SqlInsert argPkSeq(@NotNull String sequenceName) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkSeqName = sequenceName;
    SqlInsert sqlInsert = positionalArg(new RewriteArg(options.flavor().sequenceNextVal(sequenceName)));
    pkPos = parameterList.size() - 1;
    return sqlInsert;
  }

  @Override
  @NotNull
  public SqlInsert argPkSeq(@NotNull String argName, @NotNull String sequenceName) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkArgName = argName;
    pkSeqName = sequenceName;
    return namedArg(argName, new RewriteArg(options.flavor().sequenceNextVal(sequenceName)));
  }

  @Override
  @NotNull
  public SqlInsert argPkLong(String argName, Long arg) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkLong = arg;
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @NotNull
  public SqlInsert argPkLong(Long arg) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkLong = arg;
    return positionalArg(adaptor.nullNumeric(arg));
  }

  private boolean hasPk() {
    return pkArgName != null || pkSeqName != null || pkLong != null;
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
        metric.checkpoint("exec[" + numAffectedRows + "]");
        if (expectedNumAffectedRows > 0 && numAffectedRows != expectedNumAffectedRows) {
          errorCode = options.generateErrorCode();
          throw new WrongNumberOfRowsException("The number of affected rows was " + numAffectedRows + ", but "
              + expectedNumAffectedRows + " were expected." + "\n"
              + DebugSql.exceptionMessage(executeSql, parameters, errorCode, options));
        }
        isSuccess = true;
        return numAffectedRows;
      } else {
        Integer numAffectedRows = mock.insert(executeSql, DebugSql.printDebugOnlySqlString(executeSql, parameters));
        if (numAffectedRows == null) {
          // No mock behavior provided, be nice and assume the expected value
          log.debug("Setting numAffectedRows to expected");
          numAffectedRows = expectedNumAffectedRows;
        }
        metric.checkpoint("stub[" + numAffectedRows + "]");
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
        DebugSql.logSuccess("Insert", log, metric, executeSql, parameters, options);
      } else {
        DebugSql.logError("Insert", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }

  private Long updateInternal(int expectedNumAffectedRows, @NotNull String pkToReturn) {
    PreparedStatement ps = null;
    ResultSet rs = null;
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
        ps = connection.prepareStatement(executeSql, new String[] { pkToReturn });

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
        rs = ps.getGeneratedKeys();
        Long pk = null;
        if (rs != null && rs.next()) {
          pk = rs.getLong(1);
        }
        isSuccess = true;
        return pk;
      } else {
        String debugSql = DebugSql.printDebugOnlySqlString(executeSql, parameters);
        Long pk = mock.insertReturningPk(executeSql, debugSql);
        if (pk == null) {
          // No mock behavior provided, default to something that could conceivably work
          log.debug("Setting pk to hash of debugSql");
          pk = (long) debugSql.hashCode();
        }
        metric.checkpoint("stub");
        isSuccess = true;
        return pk;
      }
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw DatabaseException.wrap(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(rs, log);
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Insert", log, metric, executeSql, parameters, options);
      } else {
        DebugSql.logError("Insert", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }

  private <T> T updateInternal(int expectedNumAffectedRows, @NotNull String pkToReturn, RowsHandler<T> handler,
                              String... otherCols) {
    PreparedStatement ps = null;
    ResultSet rs = null;
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

      String[] returnCols = new String[otherCols.length + 1];
      returnCols[0] = pkToReturn;
      System.arraycopy(otherCols, 0, returnCols, 1, otherCols.length);

      if (connection != null) {
        ps = connection.prepareStatement(executeSql, returnCols);

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
        rs = ps.getGeneratedKeys();
        final ResultSet finalRs = rs;
        T result = handler.process(new RowsAdaptor(finalRs));
        metric.checkpoint("read");
        isSuccess = true;
        return result;
      } else {
        RowStub stub = mock.insertReturning(executeSql, DebugSql.printDebugOnlySqlString(executeSql, parameters));
        if (stub == null) {
          stub = new RowStub();
        }
        metric.checkpoint("stub");
        T result = handler.process(stub.toRows());
        metric.checkpoint("read");
        isSuccess = true;
        return result;
      }
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw DatabaseException.wrap(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(rs, log);
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Insert", log, metric, executeSql, parameters, options);
      } else {
        DebugSql.logError("Insert", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }

  private SqlInsert positionalArg(Object arg) {
    if (parameterList == null) {
      parameterList = new ArrayList<>();
    }
    parameterList.add(arg);
    return this;
  }

  private SqlInsert namedArg(String argName, Object arg) {
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
