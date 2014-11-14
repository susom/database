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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.Untainted;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This class is useful for dynamically generating SQL. It can "buffer" the
 * various arg*() calls and replay them later via the apply(sqlArgs) methods.
 *
 * @author garricko
 */
public class Sql implements SqlInsert.Apply, SqlUpdate.Apply, SqlSelect.Apply {
  private StringBuilder sql = new StringBuilder();
  private String finalSql;

  public Sql() {
    // Nothing to do
  }

  public Sql(@Untainted String sql) {
    this.sql.append(sql);
  }

  public Sql append(@Untainted String sql) {
    assert finalSql == null;
    this.sql.append(sql);
    return this;
  }

  @Untainted
  public String sql() {
    if (finalSql == null) {
      finalSql = sql.toString();
    }
    return finalSql;
  }

  public static enum ColumnType {
    Integer, Long, Float, Double, BigDecimal, String, ClobString, ClobStream,
    BlobBytes, BlobStream, Date, DateNowPerApp, DateNowPerDb
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
  }

  private List<Invocation> invocations = new ArrayList<>();

  @NotNull
  public Sql argInteger(@Nullable Integer arg) {
    invocations.add(new Invocation(ColumnType.Integer, null, arg));
    return this;
  }

  @NotNull
  public Sql argInteger(@NotNull String argName, @Nullable Integer arg) {
    invocations.add(new Invocation(ColumnType.Integer, argName, arg));
    return this;
  }

  @NotNull
  public Sql argLong(@Nullable Long arg) {
    invocations.add(new Invocation(ColumnType.Long, null, arg));
    return this;
  }

  @NotNull
  public Sql argLong(@NotNull String argName, @Nullable Long arg) {
    invocations.add(new Invocation(ColumnType.Long, argName, arg));
    return this;
  }

  @NotNull
  public Sql argFloat(@Nullable Float arg) {
    invocations.add(new Invocation(ColumnType.Float, null, arg));
    return this;
  }

  @NotNull
  public Sql argFloat(@NotNull String argName, @Nullable Float arg) {
    invocations.add(new Invocation(ColumnType.Float, argName, arg));
    return this;
  }

  @NotNull
  public Sql argDouble(@Nullable Double arg) {
    invocations.add(new Invocation(ColumnType.Double, null, arg));
    return this;
  }

  @NotNull
  public Sql argDouble(@NotNull String argName, @Nullable Double arg) {
    invocations.add(new Invocation(ColumnType.Double, argName, arg));
    return this;
  }

  @NotNull
  public Sql argBigDecimal(@Nullable BigDecimal arg) {
    invocations.add(new Invocation(ColumnType.BigDecimal, null, arg));
    return this;
  }

  @NotNull
  public Sql argBigDecimal(@NotNull String argName, @Nullable BigDecimal arg) {
    invocations.add(new Invocation(ColumnType.BigDecimal, argName, arg));
    return this;
  }

  @NotNull
  public Sql argString(@Nullable String arg) {
    invocations.add(new Invocation(ColumnType.String, null, arg));
    return this;
  }

  @NotNull
  public Sql argString(@NotNull String argName, @Nullable String arg) {
    invocations.add(new Invocation(ColumnType.String, argName, arg));
    return this;
  }

  @NotNull
  public Sql argDate(@Nullable Date arg) {
    invocations.add(new Invocation(ColumnType.Date, null, arg));
    return this;
  }

  @NotNull
  public Sql argDate(@NotNull String argName, @Nullable Date arg) {
    invocations.add(new Invocation(ColumnType.Date, argName, arg));
    return this;
  }

  @NotNull
  public Sql argDateNowPerApp() {
    invocations.add(new Invocation(ColumnType.DateNowPerApp, null, null));
    return this;
  }

  @NotNull
  public Sql argDateNowPerApp(@NotNull String argName) {
    invocations.add(new Invocation(ColumnType.DateNowPerApp, argName, null));
    return this;
  }

  @NotNull
  public Sql argDateNowPerDb() {
    invocations.add(new Invocation(ColumnType.DateNowPerDb, null, null));
    return this;
  }

  @NotNull
  public Sql argDateNowPerDb(@NotNull String argName) {
    invocations.add(new Invocation(ColumnType.DateNowPerDb, argName, null));
    return this;
  }

  @NotNull
  public Sql argBlobBytes(@Nullable byte[] arg) {
    invocations.add(new Invocation(ColumnType.BlobBytes, null, arg));
    return this;
  }

  @NotNull
  public Sql argBlobBytes(@NotNull String argName, @Nullable byte[] arg) {
    invocations.add(new Invocation(ColumnType.BlobBytes, argName, arg));
    return this;
  }

  @NotNull
  public Sql argBlobInputStream(@Nullable InputStream arg) {
    invocations.add(new Invocation(ColumnType.BlobStream, null, arg));
    return this;
  }

  @NotNull
  public Sql argBlobInputStream(@NotNull String argName, @Nullable InputStream arg) {
    invocations.add(new Invocation(ColumnType.BlobStream, argName, arg));
    return this;
  }

  @NotNull
  public Sql argClobString(@Nullable String arg) {
    invocations.add(new Invocation(ColumnType.ClobString, null, arg));
    return this;
  }

  @NotNull
  public Sql argClobString(@NotNull String argName, @Nullable String arg) {
    invocations.add(new Invocation(ColumnType.ClobString, argName, arg));
    return this;
  }

  @NotNull
  public Sql argClobReader(@Nullable Reader arg) {
    invocations.add(new Invocation(ColumnType.ClobStream, null, arg));
    return this;
  }

  @NotNull
  public Sql argClobReader(@NotNull String argName, @Nullable Reader arg) {
    invocations.add(new Invocation(ColumnType.ClobStream, argName, arg));
    return this;
  }

  @SuppressWarnings("CheckReturnValue")
  @Override
  public void apply(SqlSelect select) {
    for (Invocation i : invocations) {
      switch (i.columnType) {
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
}
