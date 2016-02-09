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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class is useful for dynamically generating SQL. It can "buffer" the
 * various arg*() calls and replay them later via the apply(sqlArgs) methods.
 *
 * @author garricko
 */
public class SqlArgs implements SqlInsert.Apply, SqlUpdate.Apply, SqlSelect.Apply {
  public enum ColumnType {
    Integer, Long, Float, Double, BigDecimal, String, ClobString, ClobStream,
    BlobBytes, BlobStream, Date, DateNowPerApp, DateNowPerDb, Boolean
  }

  private static class Invocation {
    ColumnType columnType;
    String argName;
    Object arg;

    Invocation(ColumnType columnType, String argName, Object arg) {
      this.columnType = columnType;
      this.argName = argName;
      this.arg = arg;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Invocation that = (Invocation) o;
      return columnType == that.columnType &&
          Objects.equals(argName, that.argName) &&
          Objects.deepEquals(arg, that.arg);
    }

    @Override
    public int hashCode() {
      return Objects.hash(columnType, argName, arg);
    }

    @Override
    public String toString() {
      return "{name=" + argName + ", type=" + columnType + ", arg=" + arg + '}';
    }
  }

  private List<Invocation> invocations = new ArrayList<>();

  @Nonnull
  public SqlArgs argBoolean(@Nullable Boolean arg) {
    invocations.add(new Invocation(ColumnType.Boolean, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argBoolean(@Nonnull String argName, @Nullable Boolean arg) {
    invocations.add(new Invocation(ColumnType.Boolean, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argInteger(@Nullable Integer arg) {
    invocations.add(new Invocation(ColumnType.Integer, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argInteger(@Nonnull String argName, @Nullable Integer arg) {
    invocations.add(new Invocation(ColumnType.Integer, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argLong(@Nullable Long arg) {
    invocations.add(new Invocation(ColumnType.Long, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argLong(@Nonnull String argName, @Nullable Long arg) {
    invocations.add(new Invocation(ColumnType.Long, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argFloat(@Nullable Float arg) {
    invocations.add(new Invocation(ColumnType.Float, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argFloat(@Nonnull String argName, @Nullable Float arg) {
    invocations.add(new Invocation(ColumnType.Float, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argDouble(@Nullable Double arg) {
    invocations.add(new Invocation(ColumnType.Double, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argDouble(@Nonnull String argName, @Nullable Double arg) {
    invocations.add(new Invocation(ColumnType.Double, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argBigDecimal(@Nullable BigDecimal arg) {
    invocations.add(new Invocation(ColumnType.BigDecimal, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argBigDecimal(@Nonnull String argName, @Nullable BigDecimal arg) {
    invocations.add(new Invocation(ColumnType.BigDecimal, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argString(@Nullable String arg) {
    invocations.add(new Invocation(ColumnType.String, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argString(@Nonnull String argName, @Nullable String arg) {
    invocations.add(new Invocation(ColumnType.String, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argDate(@Nullable Date arg) {
    invocations.add(new Invocation(ColumnType.Date, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argDate(@Nonnull String argName, @Nullable Date arg) {
    invocations.add(new Invocation(ColumnType.Date, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argDateNowPerApp() {
    invocations.add(new Invocation(ColumnType.DateNowPerApp, null, null));
    return this;
  }

  @Nonnull
  public SqlArgs argDateNowPerApp(@Nonnull String argName) {
    invocations.add(new Invocation(ColumnType.DateNowPerApp, argName, null));
    return this;
  }

  @Nonnull
  public SqlArgs argDateNowPerDb() {
    invocations.add(new Invocation(ColumnType.DateNowPerDb, null, null));
    return this;
  }

  @Nonnull
  public SqlArgs argDateNowPerDb(@Nonnull String argName) {
    invocations.add(new Invocation(ColumnType.DateNowPerDb, argName, null));
    return this;
  }

  @Nonnull
  public SqlArgs argBlobBytes(@Nullable byte[] arg) {
    invocations.add(new Invocation(ColumnType.BlobBytes, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argBlobBytes(@Nonnull String argName, @Nullable byte[] arg) {
    invocations.add(new Invocation(ColumnType.BlobBytes, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argBlobInputStream(@Nullable InputStream arg) {
    invocations.add(new Invocation(ColumnType.BlobStream, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argBlobInputStream(@Nonnull String argName, @Nullable InputStream arg) {
    invocations.add(new Invocation(ColumnType.BlobStream, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argClobString(@Nullable String arg) {
    invocations.add(new Invocation(ColumnType.ClobString, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argClobString(@Nonnull String argName, @Nullable String arg) {
    invocations.add(new Invocation(ColumnType.ClobString, argName, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argClobReader(@Nullable Reader arg) {
    invocations.add(new Invocation(ColumnType.ClobStream, null, arg));
    return this;
  }

  @Nonnull
  public SqlArgs argClobReader(@Nonnull String argName, @Nullable Reader arg) {
    invocations.add(new Invocation(ColumnType.ClobStream, argName, arg));
    return this;
  }

  @Nonnull
  public static Builder fromMetadata(Row r) {
    return new Builder(r);
  }

  /**
   * Convenience method for reading a single row. If you are reading multiple
   * rows, see {@link #fromMetadata(Row)} to avoid reading metadata multiple times.
   *
   * @return a SqlArgs with one invocation for each column in the Row, with name
   *         and type inferred from the metadata
   */
  @Nonnull
  public static SqlArgs readRow(Row r) {
    return new Builder(r).read(r);
  }

  @Nonnull
  public SqlArgs makePositional() {
    for (Invocation invocation : invocations) {
      invocation.argName = null;
    }
    return this;
  }

  @Nonnull
  public List<String> names() {
    List<String> names = new ArrayList<>();
    for (Invocation invocation : invocations) {
      if (invocation.argName != null) {
        names.add(invocation.argName);
      }
    }
    return names;
  }

  public int argCount() {
    return invocations.size();
  }

  public int positionalCount() {
    int count = 0;
    for (Invocation invocation : invocations) {
      if (invocation.argName == null) {
        count++;
      }
    }
    return count;
  }

  @SuppressWarnings("CheckReturnValue")
  @Override
  public void apply(SqlSelect select) {
    for (Invocation i : invocations) {
      switch (i.columnType) {
      case Boolean:
        if (i.argName == null) {
          select.argBoolean((Boolean) i.arg);
        } else {
          select.argBoolean(i.argName, (Boolean) i.arg);
        }
        break;
      case Integer:
        if (i.argName == null) {
          select.argInteger((Integer) i.arg);
        } else {
          select.argInteger(i.argName, (Integer) i.arg);
        }
        break;
      case Long:
        if (i.argName == null) {
          select.argLong((Long) i.arg);
        } else {
          select.argLong(i.argName, (Long) i.arg);
        }
        break;
      case Float:
        if (i.argName == null) {
          select.argFloat((Float) i.arg);
        } else {
          select.argFloat(i.argName, (Float) i.arg);
        }
        break;
      case Double:
        if (i.argName == null) {
          select.argDouble((Double) i.arg);
        } else {
          select.argDouble(i.argName, (Double) i.arg);
        }
        break;
      case BigDecimal:
        if (i.argName == null) {
          select.argBigDecimal((BigDecimal) i.arg);
        } else {
          select.argBigDecimal(i.argName, (BigDecimal) i.arg);
        }
        break;
      case String:
        if (i.argName == null) {
          select.argString((String) i.arg);
        } else {
          select.argString(i.argName, (String) i.arg);
        }
        break;
      case ClobString:
        if (i.argName == null) {
          select.argString((String) i.arg);
        } else {
          select.argString(i.argName, (String) i.arg);
        }
        break;
      case ClobStream:
        throw new DatabaseException("Don't use Clob stream parameters with select statements");
      case BlobBytes:
        throw new DatabaseException("Don't use Blob parameters with select statements");
      case BlobStream:
        throw new DatabaseException("Don't use Blob parameters with select statements");
      case Date:
        if (i.argName == null) {
          select.argDate((Date) i.arg);
        } else {
          select.argDate(i.argName, (Date) i.arg);
        }
        break;
      case DateNowPerApp:
        if (i.argName == null) {
          select.argDateNowPerApp();
        } else {
          select.argDateNowPerApp(i.argName);
        }
        break;
      case DateNowPerDb:
        if (i.argName == null) {
          select.argDateNowPerDb();
        } else {
          select.argDateNowPerDb(i.argName);
        }
        break;
      }
    }
  }

  @SuppressWarnings("CheckReturnValue")
  @Override
  public void apply(SqlInsert insert) {
    for (Invocation i : invocations) {
      switch (i.columnType) {
      case Boolean:
        if (i.argName == null) {
          insert.argBoolean((Boolean) i.arg);
        } else {
          insert.argBoolean(i.argName, (Boolean) i.arg);
        }
        break;
      case Integer:
        if (i.argName == null) {
          insert.argInteger((Integer) i.arg);
        } else {
          insert.argInteger(i.argName, (Integer) i.arg);
        }
        break;
      case Long:
        if (i.argName == null) {
          insert.argLong((Long) i.arg);
        } else {
          insert.argLong(i.argName, (Long) i.arg);
        }
        break;
      case Float:
        if (i.argName == null) {
          insert.argFloat((Float) i.arg);
        } else {
          insert.argFloat(i.argName, (Float) i.arg);
        }
        break;
      case Double:
        if (i.argName == null) {
          insert.argDouble((Double) i.arg);
        } else {
          insert.argDouble(i.argName, (Double) i.arg);
        }
        break;
      case BigDecimal:
        if (i.argName == null) {
          insert.argBigDecimal((BigDecimal) i.arg);
        } else {
          insert.argBigDecimal(i.argName, (BigDecimal) i.arg);
        }
        break;
      case String:
        if (i.argName == null) {
          insert.argString((String) i.arg);
        } else {
          insert.argString(i.argName, (String) i.arg);
        }
        break;
      case ClobString:
        if (i.argName == null) {
          insert.argClobString((String) i.arg);
        } else {
          insert.argClobString(i.argName, (String) i.arg);
        }
        break;
      case ClobStream:
        if (i.argName == null) {
          insert.argClobReader((Reader) i.arg);
        } else {
          insert.argClobReader(i.argName, (Reader) i.arg);
        }
        break;
      case BlobBytes:
        if (i.argName == null) {
          insert.argBlobBytes((byte[]) i.arg);
        } else {
          insert.argBlobBytes(i.argName, (byte[]) i.arg);
        }
        break;
      case BlobStream:
        if (i.argName == null) {
          insert.argBlobStream((InputStream) i.arg);
        } else {
          insert.argBlobStream(i.argName, (InputStream) i.arg);
        }
        break;
      case Date:
        if (i.argName == null) {
          insert.argDate((Date) i.arg);
        } else {
          insert.argDate(i.argName, (Date) i.arg);
        }
        break;
      case DateNowPerApp:
        if (i.argName == null) {
          insert.argDateNowPerApp();
        } else {
          insert.argDateNowPerApp(i.argName);
        }
        break;
      case DateNowPerDb:
        if (i.argName == null) {
          insert.argDateNowPerDb();
        } else {
          insert.argDateNowPerDb(i.argName);
        }
        break;
      }
    }
  }

  @SuppressWarnings("CheckReturnValue")
  @Override
  public void apply(SqlUpdate update) {
    for (Invocation i : invocations) {
      switch (i.columnType) {
      case Boolean:
        if (i.argName == null) {
          update.argBoolean((Boolean) i.arg);
        } else {
          update.argBoolean(i.argName, (Boolean) i.arg);
        }
        break;
      case Integer:
        if (i.argName == null) {
          update.argInteger((Integer) i.arg);
        } else {
          update.argInteger(i.argName, (Integer) i.arg);
        }
        break;
      case Long:
        if (i.argName == null) {
          update.argLong((Long) i.arg);
        } else {
          update.argLong(i.argName, (Long) i.arg);
        }
        break;
      case Float:
        if (i.argName == null) {
          update.argFloat((Float) i.arg);
        } else {
          update.argFloat(i.argName, (Float) i.arg);
        }
        break;
      case Double:
        if (i.argName == null) {
          update.argDouble((Double) i.arg);
        } else {
          update.argDouble(i.argName, (Double) i.arg);
        }
        break;
      case BigDecimal:
        if (i.argName == null) {
          update.argBigDecimal((BigDecimal) i.arg);
        } else {
          update.argBigDecimal(i.argName, (BigDecimal) i.arg);
        }
        break;
      case String:
        if (i.argName == null) {
          update.argString((String) i.arg);
        } else {
          update.argString(i.argName, (String) i.arg);
        }
        break;
      case ClobString:
        if (i.argName == null) {
          update.argClobString((String) i.arg);
        } else {
          update.argClobString(i.argName, (String) i.arg);
        }
        break;
      case ClobStream:
        if (i.argName == null) {
          update.argClobReader((Reader) i.arg);
        } else {
          update.argClobReader(i.argName, (Reader) i.arg);
        }
        break;
      case BlobBytes:
        if (i.argName == null) {
          update.argBlobBytes((byte[]) i.arg);
        } else {
          update.argBlobBytes(i.argName, (byte[]) i.arg);
        }
        break;
      case BlobStream:
        if (i.argName == null) {
          update.argBlobStream((InputStream) i.arg);
        } else {
          update.argBlobStream(i.argName, (InputStream) i.arg);
        }
        break;
      case Date:
        if (i.argName == null) {
          update.argDate((Date) i.arg);
        } else {
          update.argDate(i.argName, (Date) i.arg);
        }
        break;
      case DateNowPerApp:
        if (i.argName == null) {
          update.argDateNowPerApp();
        } else {
          update.argDateNowPerApp(i.argName);
        }
        break;
      case DateNowPerDb:
        if (i.argName == null) {
          update.argDateNowPerDb();
        } else {
          update.argDateNowPerDb(i.argName);
        }
        break;
      }
    }
  }

  public static class Builder {
    private String[] names;
    private final int[] types;
    private final int[] precision;
    private final int[] scale;

    public Builder(Row r) {
      try {
        ResultSetMetaData metadata = r.getMetadata();
        int columnCount = metadata.getColumnCount();
        names = new String[columnCount];
        types = new int[columnCount];
        precision = new int[columnCount];
        scale = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
          names[i] = metadata.getColumnName(i + 1);
          types[i] = metadata.getColumnType(i + 1);
          precision[i] = metadata.getPrecision(i + 1);
          scale[i] = metadata.getScale(i + 1);
        }

        names = tidyColumnNames(names);
      } catch (SQLException e) {
        throw new DatabaseException("Unable to retrieve metadata from ResultSet", e);
      }
    }

    @Nonnull
    public SqlArgs read(Row r) {
      SqlArgs args = new SqlArgs();

      for (int i = 0; i < names.length; i++) {
        switch (types[i]) {
        case Types.SMALLINT:
        case Types.INTEGER:
          args.argInteger(names[i], r.getIntegerOrNull());
          break;
        case Types.BIGINT:
          args.argLong(names[i], r.getLongOrNull());
          break;
        case Types.REAL:
        case 100: // Oracle proprietary it seems
          args.argFloat(names[i], r.getFloatOrNull());
          break;
        case Types.DOUBLE:
        case 101: // Oracle proprietary it seems
          args.argDouble(names[i], r.getDoubleOrNull());
          break;
        case Types.NUMERIC:
          if (precision[i] == 10 && scale[i] == 0) {
            // Oracle reports integer as numeric
            args.argInteger(names[i], r.getIntegerOrNull());
          } else if (precision[i] == 19 && scale[i] == 0) {
            // Oracle reports long as numeric
            args.argLong(names[i], r.getLongOrNull());
          } else {
            args.argBigDecimal(names[i], r.getBigDecimalOrNull());
          }
          break;
        case Types.BINARY:
        case Types.BLOB:
          args.argBlobBytes(names[i], r.getBlobBytesOrNull());
          break;
        case Types.CLOB:
        case Types.NCLOB:
          args.argClobString(names[i], r.getClobStringOrNull());
          break;
        case Types.TIMESTAMP:
          args.argDate(names[i], r.getDateOrNull());
          break;
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
          if (precision[i] >= 2147483647) {
            // Postgres seems to report clobs are varchar(2147483647)
            args.argClobString(names[i], r.getClobStringOrNull());
          } else {
            args.argString(names[i], r.getStringOrNull());
          }
          break;
        default:
          throw new DatabaseException("Don't know how to deal with column type: " + types[i]);
        }
      }
      return args;
    }
  }

  public static String[] tidyColumnNames(String[] names) {
    Set<String> uniqueNames = new LinkedHashSet<>();
    for (String name : names) {
      if (name == null || name.length() == 0) {
        name = "column_" + (uniqueNames.size() + 1);
      }
      name = name.replaceAll("[^a-zA-Z0-9]", " ");
      name = name.replaceAll("([a-z])([A-Z])", "$1_$2");
      name = name.trim().toLowerCase();
      name = name.replaceAll("\\s", "_");
      if (Character.isDigit(name.charAt(0))) {
        name = "a" + name;
      }
      if (name.length() > 29) {
        name = name.substring(0, 28);
      }
      int i = 2;
      String uniqueName = name;
      while (uniqueNames.contains(uniqueName)) {
        if (name.length() > 27) {
          name = name.substring(0, 26);
        }
        if (i > 9 && name.length() > 26) {
          name = name.substring(0, 25);
        }
        uniqueName = name + "_" + i++;
      }
      name = uniqueName;
      uniqueNames.add(name);
    }
    return uniqueNames.toArray(new String[uniqueNames.size()]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SqlArgs sqlArgs = (SqlArgs) o;
    return Objects.equals(invocations, sqlArgs.invocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(invocations);
  }

  @Override
  public String toString() {
    return "SqlArgs" + invocations;
  }
}
