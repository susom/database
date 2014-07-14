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

import java.util.ArrayList;
import java.util.List;

import com.github.susom.database.Schema.Table.Check;
import com.github.susom.database.Schema.Table.Column;
import com.github.susom.database.Schema.Table.ForeignKey;
import com.github.susom.database.Schema.Table.Index;
import com.github.susom.database.Schema.Table.Unique;

/**
 * Java representation of a database schema with the various things it can contain.
 *
 * @author garricko
 */
public class Schema {
  private List<Table> tables = new ArrayList<>();
  private List<Sequence> sequences = new ArrayList<>();

  public Sequence addSequence(String name) {
    Sequence sequence = new Sequence(name);
    sequences.add(sequence);
    return sequence;
  }

  public static enum ColumnType {
    Integer, Long, Float, Double, BigDecimal, StringVar, StringFixed, Clob, Blob, Date
  }

  public void validate() {
    for (Table t : tables) {
      t.validate();
    }
    for (Sequence s : sequences) {
      s.validate();
    }
  }

  public Table addTable(String name) {
    Table table = new Table(name);
    tables.add(table);
    return table;
  }

  public class Sequence {
    private final String name;
    private long min = 1;
    private long max = 999999999999999999L;
    private int increment = 1;
    private long start = 1;
    private int cache = 20;
    private boolean order;
    private boolean cycle;

    public Sequence(String name) {
      this.name = toName(name);
    }

    public Sequence min(long min) {
      if (start == this.min) {
        start = min;
      }
      this.min = min;
      return this;
    }

    public Sequence max(long max) {
      this.max = max;
      return this;
    }

    public Sequence increment(int increment) {
      this.increment = increment;
      return this;
    }

    public Sequence start(long start) {
      this.start = start;
      return this;
    }

    public Sequence cache(int cache) {
      this.cache = cache;
      return this;
    }

    public Sequence order() {
      order = true;
      return this;
    }

    public Sequence cycle() {
      cycle = true;
      return this;
    }

    private void validate() {

    }

    public Schema schema() {
      validate();
      return Schema.this;
    }
  }

  public class Table {
    private final String name;
    private String comment;
    private List<Column> columns = new ArrayList<>();
    private PrimaryKey primaryKey;
    private List<ForeignKey> foreignKeys = new ArrayList<>();
    private List<Index> indexes = new ArrayList<>();
    private List<Check> checks = new ArrayList<>();
    private List<Unique> uniques = new ArrayList<>();
    private Flavor customClauseFlavor;
    private String customClause;

    public Table(String name) {
      this.name = toName(name);
      if (this.name.length() > 27) {
        throw new RuntimeException("Table name should be 27 characters or less");
      }
    }

    public void validate() {
      if (columns.size() < 1) {
        throw new RuntimeException("Table " + name + " needs at least one column");
      }
      for (Column c : columns) {
        c.validate();
      }

      if (primaryKey != null) {
        primaryKey.validate();
      }

      for (ForeignKey fk : foreignKeys) {
        fk.validate();
      }

      for (Check c : checks) {
        c.validate();
      }

      for (Index i : indexes) {
        i.validate();
      }
    }

    public Schema schema() {
      validate();
      return Schema.this;
    }

    public Table withComment(String comment) {
      this.comment = comment;
      return this;
    }

    public Column addColumn(String name) {
      Column column = new Column(name);
      columns.add(column);
      return column;
    }

    public PrimaryKey addPrimaryKey(String name, String...columnNames) {
      primaryKey = new PrimaryKey(name, columnNames);
      return primaryKey;
    }

    public ForeignKey addForeignKey(String name, String...columnNames) {
      ForeignKey foreignKey = new ForeignKey(name, columnNames);
      foreignKeys.add(foreignKey);
      return foreignKey;
    }

    public Check addCheck(String name, String expression) {
      Check check = new Check(name, expression);
      checks.add(check);
      return check;
    }

    public Unique addUnique(String name, String...columnNames) {
      Unique unique = new Unique(name, columnNames);
      uniques.add(unique);
      return unique;
    }

    public Index addIndex(String name, String... columnNames) {
      Index index = new Index(name, columnNames);
      indexes.add(index);
      return index;
    }

    public Table customTableClause(Flavor flavor, String clause) {
      customClauseFlavor = flavor;
      customClause = clause;
      return this;
    }

    public class PrimaryKey {
      private final String name;
      private final List<String> columnNames = new ArrayList<>();

      public PrimaryKey(String name, String[] columnNames) {
        this.name = toName(name);
        for (String s : columnNames) {
          this.columnNames.add(toName(s));
        }
      }

      public void validate() {

      }

      public Table table() {
        validate();
        return Table.this;
      }
    }

    public class Unique {
      private final String name;
      private final List<String> columnNames = new ArrayList<>();

      public Unique(String name, String[] columnNames) {
        this.name = toName(name);
        for (String s : columnNames) {
          this.columnNames.add(toName(s));
        }
      }

      public void validate() {

      }

      public Table table() {
        validate();
        return Table.this;
      }
    }

    public class ForeignKey {
      private final String name;
      private final List<String> columnNames = new ArrayList<>();
      public String foreignTable;

      public ForeignKey(String name, String[] columnNames) {
        this.name = toName(name);
        for (String s : columnNames) {
          this.columnNames.add(toName(s));
        }
      }

      public ForeignKey references(String tableName) {
        foreignTable = toName(tableName);
        return this;
      }

      private void validate() {
        if (foreignTable == null) {
          throw new RuntimeException("Foreign key " + name + " must reference a table");
        }
      }

      public Table table() {
        validate();
        return Table.this;
      }
    }

    public class Check {
      private final String name;
      private final String expression;

      public Check(String name, String expression) {
        this.name = toName(name);
        this.expression = expression;
      }

      private void validate() {
        if (expression == null) {
          throw new RuntimeException("Expression needed for check constraint " + name + " on table " + Table.this.name);
        }
      }

      public Table table() {
        validate();
        return Table.this;
      }
    }

    public class Index {
      private final String name;
      private final List<String> columnNames = new ArrayList<>();
      private boolean unique;

      public Index(String name, String[] columnNames) {
        this.name = toName(name);
        for (String s : columnNames) {
          this.columnNames.add(toName(s));
        }
      }

      public Index unique() {
        unique = true;
        return this;
      }

      private void validate() {
        if (columnNames.size() < 1) {
          throw new RuntimeException("Index " + name + " needs at least one column");
        }
      }

      public Table table() {
        validate();
        return Table.this;
      }
    }

    public class Column {
      private final String name;
      private ColumnType type;
      private int scale;
      private int precision;
      private boolean notNull;
      private String comment;

      public Column(String name) {
        this.name = toName(name);
      }

      public Column asInteger() {
        return asType(ColumnType.Integer);
      }

      public Column asLong() {
        return asType(ColumnType.Long);
      }

      public Column asFloat() {
        return asType(ColumnType.Float);
      }

      public Column asDouble() {
        return asType(ColumnType.Double);
      }

      public Column asBigDecimal(int scale, int precision) {
        this.scale = scale;
        this.precision = precision;
        return asType(ColumnType.BigDecimal);
      }

      public Column asString(int scale) {
        this.scale = scale;
        return asType(ColumnType.StringVar);
      }

      public Column asStringFixed(int scale) {
        this.scale = scale;
        return asType(ColumnType.StringFixed);
      }

      public Column asDate() {
        return asType(ColumnType.Date);
      }

      public Column asClob() {
        return asType(ColumnType.Clob);
      }

      public Column asBlob() {
        return asType(ColumnType.Blob);
      }

      private Column asType(ColumnType type) {
        this.type = type;
        return this;
      }

      public Column notNull() {
        this.notNull = true;
        return this;
      }

      private void validate() {
        if (type == null) {
          throw new RuntimeException("Call as*() on column " + name + " table " + Table.this.name);
        }
      }

      public Table table() {
        validate();
        return Table.this;
      }

      public Column primaryKey() {
        if (type == null) {
          asLong();
        }
        if (comment == null) {
          comment = "Internally generated primary key";
        }
        notNull();
        Table.this.addPrimaryKey(Table.this.name + "_pk", name);
        return this;
      }

      public Column unique(String constraintName) {
        notNull();
        Table.this.addUnique(constraintName, name);
        return this;
      }

      public Column withComment(String comment) {
        this.comment = comment;
        return this;
      }

      public Schema schema() {
        return table().schema();
      }
    }
  }

  public void execute(Database db) {
    executeOrPrint(db, null);
  }

  public String print(Flavor flavor) {
    return executeOrPrint(null, flavor);
  }

  private String executeOrPrint(Database db, Flavor flavor) {
    validate();

    if (flavor == null) {
      flavor = db.flavor();
    }
    StringBuilder script = new StringBuilder();

    for (Table table : tables) {
      StringBuilder sql = new StringBuilder();
      sql.append("create table ").append(table.name).append(" (\n");
      boolean first = true;
      for (Column column : table.columns) {
        if (first) {
          first = false;
          sql.append("  ");
        } else {
          sql.append(",\n  ");
        }
        sql.append(rpad(column.name, 30)).append(" ");
        switch (column.type) {
          case Integer:
            sql.append(flavor.typeInteger());
            break;
          case Long:
            sql.append(flavor.typeLong());
            break;
          case Float:
            sql.append(flavor.typeFloat());
            break;
          case Double:
            sql.append(flavor.typeDouble());
            break;
          case BigDecimal:
            sql.append(flavor.typeBigDecimal(column.scale, column.precision));
            break;
          case StringVar:
            sql.append(flavor.typeStringVar(column.scale));
            break;
          case StringFixed:
            sql.append(flavor.typeStringFixed(column.scale));
            break;
          case Date:
            sql.append(flavor.typeDate());
            break;
          case Clob:
            sql.append(flavor.typeClob());
            break;
          case Blob:
            sql.append(flavor.typeBlob());
            break;
        }
        if (column.notNull) {
          sql.append(" not null");
        }
      }

      if (table.primaryKey != null) {
        sql.append(",\n  constraint ");
        sql.append(rpad(table.primaryKey.name, 30));
        sql.append(" primary key (");
        for (String name : table.primaryKey.columnNames) {
          if (sql.charAt(sql.length() - 1) == '(') {
            sql.append(name);
          } else {
            sql.append(", ");
            sql.append(name);
          }
        }
        sql.append(")");
      }

      for (Unique u : table.uniques) {
        sql.append(",\n  constraint ");
        sql.append(rpad(u.name, 30));
        sql.append(" unique (");
        for (String name : u.columnNames) {
          if (sql.charAt(sql.length() - 1) == '(') {
            sql.append(name);
          } else {
            sql.append(", ");
            sql.append(name);
          }
        }
        sql.append(")");
      }

      for (Check check : table.checks) {
        sql.append(",\n  constraint ");
        sql.append(rpad(check.name, 30));
        sql.append(" check (");
        sql.append(check.expression);
        sql.append(")");
      }

      sql.append("\n)");
      if (table.customClause != null && flavor == table.customClauseFlavor) {
        sql.append(" ").append(table.customClause);
      }
      executeOrPrint(sql, db, script);

      if (flavor == Flavor.oracle || flavor == Flavor.postgresql) {
        if (table.comment != null) {
          sql.append("comment on table ");
          sql.append(table.name);
          sql.append(" is \n'");
          sql.append(table.comment.replace("\'", "\'\'"));
          sql.append("'");
          executeOrPrint(sql, db, script);
        }

        for (Column c : table.columns) {
          if (c.comment != null) {
            sql.append("comment on column ");
            sql.append(table.name);
            sql.append(".");
            sql.append(c.name);
            sql.append(" is \n'");
            sql.append(c.comment.replace("\'", "\'\'"));
            sql.append("'");
            executeOrPrint(sql, db, script);
          }
        }
      }
    }

    for (Table table : tables) {
      StringBuilder sql = new StringBuilder();

      for (ForeignKey fk : table.foreignKeys) {
        sql.append("alter table ");
        sql.append(table.name);
        sql.append(" add constraint ");
        sql.append(fk.name);
        sql.append("\n  foreign key (");
        for (String name : fk.columnNames) {
          if (sql.charAt(sql.length() - 1) == '(') {
            sql.append(name);
          } else {
            sql.append(", ");
            sql.append(name);
          }
        }
        sql.append(") references ");
        sql.append(fk.foreignTable);
        executeOrPrint(sql, db, script);
      }
    }

    for (Table table : tables) {
      StringBuilder sql = new StringBuilder();

      for (Index index : table.indexes) {
        sql.append("create index ");
        sql.append(index.name);
        sql.append(" on ");
        sql.append(table.name);
        sql.append(" (");
        for (String name : index.columnNames) {
          if (sql.charAt(sql.length() - 1) == '(') {
            sql.append(name);
          } else {
            sql.append(", ");
            sql.append(name);
          }
        }
        sql.append(")");
        executeOrPrint(sql, db, script);
      }
    }

    for (Sequence sequence : sequences) {
      StringBuilder sql = new StringBuilder();

      sql.append("create sequence ");
      sql.append(sequence.name);
      if (flavor == Flavor.derby) {
        sql.append(" as bigint");
      }
      sql.append(" minvalue ");
      sql.append(sequence.min);
      sql.append(" maxvalue ");
      sql.append(sequence.max);
      sql.append(" start with ");
      sql.append(sequence.start);
      sql.append(" increment by ");
      sql.append(sequence.increment);
      sql.append(flavor.sequenceCacheClause(sequence.cache));
      sql.append(flavor.sequenceOrderClause(sequence.order));
      sql.append(flavor.sequenceCycleClause(sequence.cycle));
      executeOrPrint(sql, db, script);
    }

    if (db == null) {
      return script.toString();
    }
    return null;
  }

  private void executeOrPrint(StringBuilder sql, Database db, StringBuilder script) {
    if (db != null) {
      db.ddl(sql.toString()).execute();
    } else {
      script.append(sql.toString());
      script.append(";\n\n");
    }
    sql.setLength(0);
  }

  private String toName(String name) {
    name = name.toLowerCase().trim();

    if (!name.matches("[a-z][a-z0-9_]{0,28}[a-z0-9]?")) {
      throw new IllegalArgumentException("Identifier name should match pattern [a-z][a-z0-9_]{0,28}[a-z0-9]?");
    }

    return name;
  }

  private String rpad(String s, int size) {
    if (s.length() < size) {
      s += "                                                       ".substring(0, size - s.length());
    }
    return s;
  }
}
