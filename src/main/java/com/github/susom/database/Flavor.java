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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Enumeration of supported databases with various compatibility settings.
 *
 * @author garricko
 */

// Default identifier case supported by the database when identifier is not in quotes.
// See https://github.com/ontop/ontop/wiki/Case-sensitivity-for-SQL-identifiers
public enum Flavor {
  derby {
    @Override
    public boolean isNormalizedUpperCase() {
      return true;
    }

    @Override
    public String typeInteger() {
      return "integer";
    }

    @Override
    public String typeBoolean() {
      return "char(1)";
    }

    @Override
    public String typeLong() {
      return "bigint";
    }

    @Override
    public String typeFloat() {
      return "real";
    }

    @Override
    public String typeDouble() {
      return "double";
    }

    @Override
    public String typeBigDecimal(int size, int precision) {
      return "numeric(" + size + "," + precision + ")";
    }

    @Override
    public String typeStringVar(int length) {
      return "varchar(" + length + ")";
    }

    @Override
    public String typeStringFixed(int length) {
      return "char(" + length + ")";
    }

    @Override
    public String typeClob() {
      return "clob";
    }

    @Override
    public String typeBlob() {
      return "blob";
    }

    @Override
    public String typeDate() {
      return "timestamp";
    }

    @Override
    public String typeLocalDate() {
      return "date";
    }

    @Override
    public boolean useStringForClob() {
      return false;
    }

    @Override
    public boolean useBytesForBlob() {
      return false;
    }

    @Override
    public String sequenceNextVal(String sequenceName) {
      return "next value for " + sequenceName;
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      return "values next value for " + sequenceName;
    }

    @Override
    public String sequenceDrop(String dbtestSeq) {
      return "drop sequence " + dbtestSeq + " restrict";
    }

    @Override
    public boolean supportsInsertReturning() {
      return false;
    }

    @Override
    public String sequenceCacheClause(int nbrValuesToCache) {
      return "";
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      return "";
    }

    @Override
    public String sequenceCycleClause(boolean cycle) {
      return cycle ? " cycle" : " no cycle";
    }

    @Override
    public String dbTimeMillis() {
      return "current_timestamp";
    }

    @Override
    public String fromAny() {
      return " from sysibm.sysdummy1";
    }

    @Override
    public String dateAsSqlFunction(Date date, Calendar calendar) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setCalendar(calendar);
      return "timestamp('" + dateFormat.format(date) + "')";
    }

    @Override
    public String localDateAsSqlFunction(Date date) {
      return "'" + date.toString() + "'";
    }

    @Override
    public String sequenceOptions() {
      return " as bigint";
    }

    @Override
    public boolean autoCommitOnly() {
      return false;
    }
  },
  sqlserver {
    @Override
    public boolean isNormalizedUpperCase() {
      return false;
    }

    @Override
    public String typeFloat() {
      return "float(24)";
    }

    @Override
    public String typeDouble() {
      return "float(53)";
    }

    @Override
    public String typeBigDecimal(int size, int precision) {
      return "numeric(" + size + "," + precision + ")";
    }

    @Override
    public String typeInteger() {
      return "numeric(10)";
    }

    @Override
    public String typeBoolean() {
      return "char(1)";
    }

    @Override
    public String typeLong() {
      return "numeric(19)";
    }

    @Override
    public String typeDate() {
      return "datetime2(3)";
    }

    @Override
    public String typeLocalDate() {
      return "date";
    }

    @Override
    public boolean useStringForClob() {
      return false;
    }

    @Override
    public boolean useBytesForBlob() {
      return false;
    }

    @Override
    public String sequenceNextVal(String sequenceName) {
      return "next value for " + sequenceName;
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      return "select next value for " + sequenceName;
    }

    @Override
    public String sequenceDrop(String dbtestSeq) {
      return "drop sequence " + dbtestSeq;
    }

    @Override
    public String typeStringVar(int length) {
      return "varchar(" + length + ")";
    }

    @Override
    public String typeStringFixed(int length) {
      return "char(" + length + ")";
    }

    @Override
    public String typeClob() {
      return "varchar(max)";
    }

    @Override
    public String typeBlob() {
      return "varbinary(max)";
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      // Not supported
      return "";
    }

    @Override
    public String sequenceCycleClause(boolean cycle) {
      return cycle ? " cycle" : " no cycle";
    }

    @Override
    public boolean supportsInsertReturning() {
      // TODO it probably does, but I haven't figure it out yet
      return false;
    }

    @Override
    public String dbTimeMillis() {
      return "current_timestamp";
    }

    @Override
    public String sequenceCacheClause(int nbrValuesToCache) {
      if (nbrValuesToCache < 2) {
        return " no cache";
      }
      return " cache " + nbrValuesToCache;
    }

    @Override
    public String fromAny() {
      return "";
    }

    @Override
    public String dateAsSqlFunction(Date date, Calendar calendar) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setCalendar(calendar);
      return "cast('" + dateFormat.format(date) + "' as datetime2(3))";
    }

    @Override
    public String localDateAsSqlFunction(Date date) {
      return "'" + date.toString() + "'";
    }

    @Override
    public String sequenceOptions() {
      return "";
    }

    @Override
    public boolean autoCommitOnly() {
      return false;
    }
  },
  oracle {
    @Override
    public boolean isNormalizedUpperCase() {
      return true;
    }

    @Override
    public String typeFloat() {
      return "binary_float";
    }

    @Override
    public String typeDouble() {
      return "binary_double";
    }

    @Override
    public String typeBigDecimal(int size, int precision) {
      return "numeric(" + size + "," + precision + ")";
    }

    @Override
    public String typeInteger() {
      return "numeric(10)";
    }

    @Override
    public String typeBoolean() {
      return "char(1 char)";
    }

    @Override
    public String typeLong() {
      return "numeric(19)";
    }

    @Override
    public String typeDate() {
      return "timestamp(3)";
    }

    @Override
    public String typeLocalDate() {
      return "date";
    }

    @Override
    public boolean useStringForClob() {
      return false;
    }

    @Override
    public boolean useBytesForBlob() {
      return false;
    }

    @Override
    public String sequenceNextVal(String sequenceName) {
      return sequenceName + ".nextval";
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      return "select " + sequenceName + ".nextval from dual";
    }

    @Override
    public String sequenceDrop(String dbtestSeq) {
      return "drop sequence " + dbtestSeq;
    }

    @Override
    public String typeStringVar(int length) {
      return "varchar2(" + length + " char)";
    }

    @Override
    public String typeStringFixed(int length) {
      return "char(" + length + " char)";
    }

    @Override
    public String typeClob() {
      return "clob";
    }

    @Override
    public String typeBlob() {
      return "blob";
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      return order ? " order" : " noorder";
    }

    @Override
    public String sequenceCycleClause(boolean cycle) {
      return cycle ? " cycle" : " nocycle";
    }

    @Override
    public boolean supportsInsertReturning() {
      return true;
    }

    @Override
    public String dbTimeMillis() {
      return "systimestamp(3)";
    }

    @Override
    public String sequenceCacheClause(int nbrValuesToCache) {
      if (nbrValuesToCache < 2) {
        return " nocache";
      }
      return " cache " + nbrValuesToCache;
    }

    @Override
    public String fromAny() {
      return " from dual";
    }

    @Override
    public String dateAsSqlFunction(Date date, Calendar calendar) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setCalendar(calendar);
      return "timestamp '" + dateFormat.format(date) + "'";
    }

    @Override
    public String localDateAsSqlFunction(Date date) {
      return "to_date('" + date.toString() + "', 'yyyy-mm-dd')";
    }

    @Override
    public String sequenceOptions() {
      return "";
    }

    @Override
    public boolean autoCommitOnly() {
      return false;
    }
  },
  postgresql {
    @Override
    public boolean isNormalizedUpperCase() {
      return false;
    }

    @Override
    public String typeInteger() {
      return "integer";
    }

    @Override
    public String typeBoolean() {
      return "char(1)";
    }

    @Override
    public String typeLong() {
      return "bigint";
    }

    @Override
    public String typeFloat() {
      return "real";
    }

    @Override
    public String typeDouble() {
      return "double precision";
    }

    @Override
    public String typeBigDecimal(int size, int precision) {
      return "numeric(" + size + "," + precision + ")";
    }

    @Override
    public String typeStringVar(int length) {
      return "varchar(" + length + ")";
    }

    @Override
    public String typeStringFixed(int length) {
      return "char(" + length + ")";
    }

    @Override
    public String typeClob() {
      return "text";
    }

    @Override
    public String typeBlob() {
      return "bytea";
    }

    @Override
    public String typeDate() {
      return "timestamp(3)";
    }

    @Override
    public String typeLocalDate() {
      return "date";
    }

    @Override
    public boolean useStringForClob() {
      return true;
    }

    @Override
    public boolean useBytesForBlob() {
      return true;
    }

    @Override
    public String sequenceNextVal(String sequenceName) {
      return "nextval('" + sequenceName + "')";
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      return "select nextval('" + sequenceName + "')";
    }

    @Override
    public String sequenceDrop(String dbtestSeq) {
      return "drop sequence " + dbtestSeq;
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      return "";
    }

    @Override
    public String sequenceCycleClause(boolean cycle) {
      return cycle ? " cycle" : " no cycle";
    }

    @Override
    public String fromAny() {
      return "";
    }

    @Override
    public boolean supportsInsertReturning() {
      return true;
    }

    @Override
    public String dbTimeMillis() {
      return "date_trunc('milliseconds',localtimestamp)";
    }

    @Override
    public String sequenceCacheClause(int nbrValuesToCache) {
      return " cache " + nbrValuesToCache;
    }

    @Override
    public String dateAsSqlFunction(Date date, Calendar calendar) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setCalendar(calendar);
      return "'" + dateFormat.format(date) + " GMT'::timestamp";
    }

    @Override
    public String localDateAsSqlFunction(Date date) {
      return "'" + date.toString()+"'";
    }

    @Override
    public String sequenceOptions() {
      return "";
    }

    @Override
    public boolean autoCommitOnly() {
      return false;
    }
  },
  hsqldb {
    @Override
    public boolean isNormalizedUpperCase() {
      return true;
    }

    @Override
    public String typeInteger() {
      return "integer";
    }

    @Override
    public String typeBoolean() {
      return "char(1)";
    }

    @Override
    public String typeLong() {
      return "bigint";
    }

    @Override
    public String typeFloat() {
      return "double";
    }

    @Override
    public String typeDouble() {
      return "double";
    }

    @Override
    public String typeBigDecimal(int size, int precision) {
      return "numeric(" + size + "," + precision + ")";
    }

    @Override
    public String typeStringVar(int length) {
      return "varchar(" + length + ")";
    }

    @Override
    public String typeStringFixed(int length) {
      return "char(" + length + ")";
    }

    @Override
    public String typeClob() {
      return "clob(2G)";
    }

    @Override
    public String typeBlob() {
      return "blob(2G)";
    }

    @Override
    public String typeDate() {
      return "timestamp(3)";
    }

    @Override
    public String typeLocalDate() {
      return "date";
    }

    @Override
    public boolean useStringForClob() {
      return true;
    }

    @Override
    public boolean useBytesForBlob() {
      return true;
    }

    @Override
    public String sequenceNextVal(String sequenceName) {
      return "next value for " + sequenceName + "";
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      return "select " + sequenceNextVal(sequenceName) + fromAny();
    }

    @Override
    public String sequenceDrop(String dbtestSeq) {
      return "drop sequence if exists " + dbtestSeq;
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      return "";
    }

    @Override
    public String sequenceCycleClause(boolean cycle) {
      return cycle ? " cycle" : " no cycle";
    }

    @Override
    public String fromAny() {
      return " from (values(0))";
    }

    @Override
    public boolean supportsInsertReturning() {
      return false;
    }

    @Override
    public String dbTimeMillis() {
      return "localtimestamp";
    }

    @Override
    public String sequenceCacheClause(int nbrValuesToCache) {
      return "";
    }

    @Override
    public String dateAsSqlFunction(Date date, Calendar calendar) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000XXX");
      dateFormat.setCalendar(calendar);
      return "cast(timestamp '" + dateFormat.format(date) + "' as timestamp without time zone)";
    }

    @Override
    public String localDateAsSqlFunction(Date date) {
      return "'" + date.toString()+"'";
    }

    @Override
    public String sequenceOptions() {
      return " as bigint";
    }

    @Override
    public boolean autoCommitOnly() {
      return false;
    }
  },
  bigquery {
    @Override
    public boolean isNormalizedUpperCase() {
      return false;
    }

    @Override
    public String typeInteger() {
      return "int64";
    }

    @Override
    public String typeBoolean() {
      // BigQuery has a native boolean type, but we're not trying to use it
      return "string";
    }

    @Override
    public String typeLong() {
      return "int64";
    }

    @Override
    public String typeFloat() {
      return "float64";
    }

    @Override
    public String typeDouble() {
      return "float64";
    }

    @Override
    public String typeBigDecimal(int size, int precision) {
      return "numeric";
    }

    @Override
    public String typeStringVar(int length) {
      return "string";
    }

    @Override
    public String typeStringFixed(int length) {
      return "string";
    }

    @Override
    public String typeClob() {
      return "string";
    }

    @Override
    public String typeBlob() {
      return "bytes";
    }

    @Override
    public String typeDate() {
      return "datetime";
    }

    @Override
    public String typeLocalDate() {
      return "date";
    }

    @Override
    public boolean useStringForClob() {
      return true;
    }

    @Override
    public boolean useBytesForBlob() {
      return true;
    }

    @Override
    public String sequenceNextVal(String sequenceName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String sequenceDrop(String dbtestSeq) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsInsertReturning() {
      return false;
    }

    @Override
    public String dbTimeMillis() {
      return "current_timestamp()";
    }

    @Override
    public String sequenceCacheClause(int nbrValuesToCache) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String sequenceCycleClause(boolean cycle) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String fromAny() {
      return "";
    }

    @Override
    public String dateAsSqlFunction(Date date, Calendar calendar) {
      // Construct a datetime literal
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setCalendar(calendar);
      return String.format("datetime '%s'", dateFormat.format(date));
    }

    @Override
    public String localDateAsSqlFunction(Date date) {
      // Construct a datetime literal
      return String.format("datetime '%s'", date.toString());
    }

    @Override
    public String sequenceOptions() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean autoCommitOnly() {
      return true;
    }
  };

  // Returns true if DB normalizes to upper case names for ids like tables and columns
  // See https://github.com/ontop/ontop/wiki/Case-sensitivity-for-SQL-identifiers
  public abstract boolean isNormalizedUpperCase();

  public abstract String typeInteger();

  public abstract String typeBoolean();

  public abstract String typeLong();

  public abstract String typeFloat();

  public abstract String typeDouble();

  public abstract String typeBigDecimal(int size, int precision);

  public abstract String typeStringVar(int length);

  public abstract String typeStringFixed(int length);

  public abstract String typeClob();

  public abstract String typeBlob();

  public abstract String typeDate();

  public abstract String typeLocalDate();

  public abstract boolean useStringForClob();

  public abstract boolean useBytesForBlob();

  public abstract String sequenceNextVal(String sequenceName);

  public abstract String sequenceSelectNextVal(String sequenceName);

  public abstract String sequenceDrop(String dbtestSeq);

  public abstract boolean supportsInsertReturning();

  public abstract String dbTimeMillis();

  public abstract String sequenceCacheClause(int nbrValuesToCache);

  public abstract String sequenceOrderClause(boolean order);

  public abstract String sequenceCycleClause(boolean cycle);

  /**
   * Indicate what should follow a constant select statement. For example, "select 1"
   * works on some databases, while Oracle requires "select 1 from dual". For Oracle
   * this function should return " from dual" (including the leading space).
   */
  public abstract String fromAny();

  /**
   * Return a SQL function representing the specified date. For example, in PostgreSQL this
   * looks like "'1970-01-02 02:17:36.789000 GMT'::timestamp".
   */
  public abstract String dateAsSqlFunction(Date date, Calendar calendar);

/**
 * Return a SQL function representing the specified date without time.
 */
  public abstract String localDateAsSqlFunction(Date date);

  public abstract String sequenceOptions();

  public abstract boolean autoCommitOnly();

  public static Flavor fromJdbcUrl(String url) {
    if (url.startsWith("jdbc:postgresql:")) {
      return postgresql;
    } else if (url.startsWith("jdbc:oracle:")) {
      return oracle;
    } else if (url.startsWith("jdbc:sqlserver:")) {
      return sqlserver;
    } else if (url.startsWith("jdbc:hsqldb:")) {
      return hsqldb;
    } else if (url.startsWith("jdbc:derby:")) {
      return derby;
    } else if (url.startsWith("jdbc:bigquery:")) {
      return bigquery;
    } else {
      throw new DatabaseException("Cannot determine database flavor from url");
    }
  }

  public static String driverForJdbcUrl(String url) {
    if (url.startsWith("jdbc:postgresql:")) {
      return "org.postgresql.Driver";
    } else if (url.startsWith("jdbc:oracle:")) {
      return "oracle.jdbc.OracleDriver";
    } else if (url.startsWith("jdbc:sqlserver:")) {
      return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    } else if (url.startsWith("jdbc:hsqldb:")) {
      return "org.hsqldb.jdbc.JDBCDriver";
    } else if (url.startsWith("jdbc:derby:")) {
      return "org.apache.derby.jdbc.EmbeddedDriver";
    } else if (url.startsWith("jdbc:bigquery:")) {
      return "com.simba.googlebigquery.jdbc42.Driver";
    } else {
      throw new DatabaseException("Cannot determine database driver class from url");
    }
  }
}
