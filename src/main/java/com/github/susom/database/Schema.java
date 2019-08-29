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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.function.Supplier;

import com.github.susom.database.Schema.Table.Check;
import com.github.susom.database.Schema.Table.Column;
import com.github.susom.database.Schema.Table.ForeignKey;
import com.github.susom.database.Schema.Table.Index;
import com.github.susom.database.Schema.Table.Unique;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java representation of a database schema with the various things it can contain.
 *
 * @author garricko
 */
public class Schema {
  private List<Table> tables = new ArrayList<>();
  private List<Sequence> sequences = new ArrayList<>();
  private boolean indexForeignKeys = true;
  private String userTableName = "user_principal";

  private static final Logger log = LoggerFactory.getLogger(Schema.class);

  public Sequence addSequence(String name) {
    Sequence sequence = new Sequence(name);
    sequences.add(sequence);
    return sequence;
  }

  public Schema withoutForeignKeyIndexing() {
    indexForeignKeys = false;
    return this;
  }

  /**
   * Set the table to which the foreign key will be created for
   * user change tracking ({@link Table#trackCreateTimeAndUser(String)}
   * and {@link Table#trackUpdateTimeAndUser(String)}).
   *
   * @param userTableName the default table name containing users
   */
  public Schema userTableName(String userTableName) {
    this.userTableName = userTableName;
    return this;
  }

  public enum ColumnType {
    Integer, Long, Float, Double, BigDecimal, StringVar, StringFixed, Clob, Blob, Date, LocalDate, Boolean
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

  public Table addTableFromRow(String tableName, Row r) {
    Table table = addTable(tableName);
    try {
      ResultSetMetaData metadata = r.getMetadata();

      int columnCount = metadata.getColumnCount();
      String[] names = new String[columnCount];
      for (int i = 0; i < columnCount; i++) {
        names[i] = metadata.getColumnName(i + 1);
      }
      names = SqlArgs.tidyColumnNames(names);

      for (int i = 0; i < columnCount; i++) {
        int type = metadata.getColumnType(i + 1);

        switch (type) {
        case Types.SMALLINT:
        case Types.INTEGER:
          table.addColumn(names[i]).asInteger();
          break;
        case Types.BIGINT:
          table.addColumn(names[i]).asLong();
          break;
        case Types.REAL:
        case 100: // Oracle proprietary it seems
          table.addColumn(names[i]).asFloat();
          break;
        case Types.DOUBLE:
        case 101: // Oracle proprietary it seems
          table.addColumn(names[i]).asDouble();
          break;
        case Types.NUMERIC:
          int precision1 = metadata.getPrecision(i + 1);
          int scale = metadata.getScale(i + 1);
          if (precision1 == 10 && scale == 0) {
            // Oracle reports integer as numeric
            table.addColumn(names[i]).asInteger();
          } else if (precision1 == 19 && scale == 0) {
            // Oracle reports long as numeric
            table.addColumn(names[i]).asLong();
          } else if (precision1 == 126 && scale == -127) {
            // this clause was added to support ETL from MSSQL Server
            table.addColumn(names[i]).asFloat();
          } else if (precision1 == 0 && scale == -127) {
            // this clause was also added to support ETL from MSSQL Server
            table.addColumn(names[i]).asInteger();
          } else {
            table.addColumn(names[i]).asBigDecimal(precision1, scale);
          }
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.BLOB:
          table.addColumn(names[i]).asBlob();
          break;
        case Types.CLOB:
        case Types.NCLOB:
          table.addColumn(names[i]).asClob();
          break;

        // The date type is used for a true date - no time info.
        // It must be checked before TimeStamp because sql dates are also
        // recognized as sql timestamp.
        case Types.DATE:
          table.addColumn(names[i]).asLocalDate();
          break;

        // This is the type dates and times with time and time zone associated.
        // Note that Oracle dates are always really Timestamps.
        case Types.TIMESTAMP:
          // Old DBs like Oracle do not have a date type.  Date is implemented as a timestamp
          // In this case, we will look to see if the time is exactly midnight down to nanosecond
          // to determine if it is really a LocalDate.
          if (r.isMidnight(names[i])) {
            log.warn("Processing Oracle DB column "+names[i]+" as a LocalDate because time was 0");
            table.addColumn(names[i]).asLocalDate();
          } else {
            table.addColumn(names[i]).asDate();
          }
          break;

        case Types.NVARCHAR:
        case Types.VARCHAR:
          int precision = metadata.getPrecision(i + 1);
          if (precision >= 2147483647) {
            // Postgres seems to report clobs are varchar(2147483647)
            table.addColumn(names[i]).asClob();
          } else {
            table.addColumn(names[i]).asString(precision);
          }
          break;
        case Types.CHAR:
        case Types.NCHAR:
          table.addColumn(names[i]).asStringFixed(metadata.getPrecision(i + 1));
          break;
        default:
          throw new DatabaseException("Don't know what type to use for: " + type);
        }
      }
    } catch (SQLException e) {
      throw new DatabaseException("Unable to retrieve metadata from ResultSet", e);
    }
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

    /**
     * On databases that support it, indicate you want to strictly order the values returned
     * from the sequence. This is generally NOT what you want, because it can dramatically
     * reduce performance (requires locking and synchronization). Also keep in mind it doesn't
     * guarantee there will not be gaps in the numbers handed out (nothing you can do will
     * ever prevent that).
     */
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
    private Map<Flavor, String> customClauses = new HashMap<>();
    private boolean createTracking;
    private String createTrackingFkName;
    private String createTrackingFkTable;
    private boolean updateTracking;
    private String updateTrackingFkName;
    private String updateTrackingFkTable;
    private boolean updateSequence;
    private boolean historyTable;

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
      if (createTracking) {
        addColumn("create_time").asDate().table();
      }
      if (createTrackingFkName != null) {
        addColumn("create_user").foreignKey(createTrackingFkName).references(createTrackingFkTable).table();
      }
      if (updateTracking || updateSequence) {
        addColumn("update_time").asDate().table();
      }
      if (updateTrackingFkName != null) {
        addColumn("update_user").foreignKey(updateTrackingFkName).references(updateTrackingFkTable).table();
      }
      if (updateSequence) {
        addColumn("update_sequence").asLong().table();
      }
      // Avoid auto-indexing foreign keys if an index already exists (the first columns of the pk or explicit index)
      if (indexForeignKeys) {
        for (ForeignKey fk : foreignKeys) {
          if (primaryKey != null && 0 == Collections.indexOfSubList(primaryKey.columnNames, fk.columnNames)) {
            continue;
          }
          boolean skip = false;
          for (Index i : indexes) {
            if (0 == Collections.indexOfSubList(i.columnNames, fk.columnNames)) {
              skip = true;
              break;
            }
          }
          if (!skip) {
            addIndex(fk.name + "_ix", fk.columnNames.toArray(new String[fk.columnNames.size()]));
          }
        }
      }
      validate();
      if (historyTable) {
        String historyTableName = name + "_history";
        if (historyTableName.length() > 27 && historyTableName.length() <= 30) {
          historyTableName = name + "_hist";
        }
        Table hist = Schema.this.addTable(historyTableName);
        // History table needs all the same columns as the original
        hist.columns.addAll(columns);
        // Add a synthetic column to indicate when the original row has been deleted
        hist.addColumn("is_deleted").asBoolean().table();
        List<String> pkColumns = new ArrayList<>();
        pkColumns.addAll(primaryKey.columnNames);
        // Index the primary key from the regular table for retrieving history
        hist.addIndex(historyTableName + "_ix", pkColumns.toArray(new String[pkColumns.size()]));
        // The primary key for the history table will be that of the original table, plus the update sequence
        pkColumns.add("update_sequence");
        hist.addPrimaryKey(historyTableName + "_pk", pkColumns.toArray(new String[pkColumns.size()]));
        // To perform any validation
        hist.schema();
      }
      return Schema.this;
    }

    public Table withComment(String comment) {
      this.comment = comment;
      return this;
    }

    public Table withStandardPk() {
      return addColumn(name + "_id").primaryKey().table();
    }

    public Table trackCreateTime() {
      createTracking = true;
      return this;
    }

    public Table trackCreateTimeAndUser(String fkConstraintName) {
      return trackCreateTimeAndUser(fkConstraintName, userTableName);
    }

    public Table trackCreateTimeAndUser(String fkConstraintName, String fkReferencesTable) {
      createTracking = true;
      createTrackingFkName = fkConstraintName;
      createTrackingFkTable = fkReferencesTable;
      return this;
    }

    public Table trackUpdateTime() {
      updateTracking = true;
      updateSequence = true;
      return this;
    }

    public Table trackUpdateTimeAndUser(String fkConstraintName) {
      return trackUpdateTimeAndUser(fkConstraintName, userTableName);
    }

    public Table trackUpdateTimeAndUser(String fkConstraintName, String fkReferencesTable) {
      updateTracking = true;
      updateSequence = true;
      updateTrackingFkName = fkConstraintName;
      updateTrackingFkTable = fkReferencesTable;
      return this;
    }

    public Table withHistoryTable() {
      updateSequence = true;
      historyTable = true;
      return this;
    }

    public Column addColumn(String name) {
      Column column = new Column(name);
      columns.add(column);
      return column;
    }

    public PrimaryKey addPrimaryKey(String name, String...columnNames) {
      if (primaryKey != null) {
        throw new RuntimeException("Only one primary key is allowed. For composite keys use"
            + " addPrimaryKey(name, c1, c2, ...).");
      }
      for (Column c: columns) {
        if (c.name.equalsIgnoreCase(name)) {
          throw new RuntimeException("For table: " + this.name + " primary key name should not be a column name: " + name);
        }
      }
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
      customClauses.put(flavor, clause);
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
      private boolean onDeleteCascade = false;
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

      public ForeignKey onDeleteCascade() {
        onDeleteCascade = true;
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

      /**
       * Create a boolean column, usually char(1) to hold values 'Y' or 'N'. This
       * parameterless version does not create any check constraint at the database
       * level.
       */
      public Column asBoolean() {
        return asType(ColumnType.Boolean);
      }

      /**
       * Create a boolean column, usually char(1) to hold values 'Y' or 'N'. This
       * version creates a check constraint at the database level with the provided name.
       */
      public Column asBoolean(String checkConstraintName) {
        return asBoolean().check(checkConstraintName, name + " in ('Y', 'N')");
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

      // This type is for dates that have time associated
      public Column asDate() {
        return asType(ColumnType.Date);
      }

      // This type is for true dates with no time associated
      public Column asLocalDate() {
        return asType(ColumnType.LocalDate);
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

      public ForeignKey foreignKey(String constraintName) {
        if (type == null) {
          asLong();
        }
        return table().addForeignKey(constraintName, name);
      }

      public Column check(String checkConstraintName, String expression) {
        table().addCheck(checkConstraintName, expression).table();
        return this;
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

  public void execute(Supplier<Database> db) {
    executeOrPrint(db.get(), null);
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
      Sql sql = new Sql();
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
          case Boolean:
            sql.append(flavor.typeBoolean());
            break;
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
            sql.append(flavor.typeDate());      // Append a date with time
            break;
          case LocalDate:
            sql.append(flavor.typeLocalDate()); // Append a true date - no time
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
        sql.listStart(" primary key (");
        for (String name : table.primaryKey.columnNames) {
          sql.listSeparator(", ");
          sql.append(name);
        }
        sql.listEnd(")");
      }

      for (Unique u : table.uniques) {
        sql.append(",\n  constraint ");
        sql.append(rpad(u.name, 30));
        sql.listStart(" unique (");
        for (String name : u.columnNames) {
          sql.listSeparator(", ");
          sql.append(name);
        }
        sql.listEnd(")");
      }

      for (Check check : table.checks) {
        sql.append(",\n  constraint ");
        sql.append(rpad(check.name, 30));
        sql.append(" check (");
        sql.append(check.expression);
        sql.append(")");
      }

      sql.append("\n)");
      if (table.customClauses.containsKey(flavor)) {
        sql.append(" ").append(table.customClauses.get(flavor));
      }
      executeOrPrint(sql, db, script);
      sql = new Sql();

      if (flavor == Flavor.oracle || flavor == Flavor.postgresql) {
        if (table.comment != null) {
          sql.append("comment on table ");
          sql.append(table.name);
          sql.append(" is \n'");
          sql.append(table.comment.replace("\'", "\'\'"));
          sql.append("'");
          executeOrPrint(sql, db, script);
          sql = new Sql();
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
            sql = new Sql();
          }
        }
      }
    }

    for (Table table : tables) {
      for (ForeignKey fk : table.foreignKeys) {
        Sql sql = new Sql();
        sql.append("alter table ");
        sql.append(table.name);
        sql.append(" add constraint ");
        sql.append(fk.name);
        sql.listStart("\n  foreign key (");
        for (String name : fk.columnNames) {
          sql.listSeparator(", ");
          sql.append(name);
        }
        sql.listEnd(") references ");
        sql.append(fk.foreignTable);
        if (fk.onDeleteCascade) {
          sql.append(" on delete cascade");
        }
        executeOrPrint(sql, db, script);
      }
    }

    for (Table table : tables) {
      for (Index index : table.indexes) {
        Sql sql = new Sql();
        sql.append("create ");
        if (index.unique) {
          sql.append("unique ");
        }
        sql.append("index ");
        sql.append(index.name);
        sql.append(" on ");
        sql.append(table.name);
        sql.listStart(" (");
        for (String name : index.columnNames) {
          sql.listSeparator(", ");
          sql.append(name);
        }
        sql.listEnd(")");
        executeOrPrint(sql, db, script);
      }
    }

    for (Sequence sequence : sequences) {
      Sql sql = new Sql();
      sql.append("create sequence ");
      sql.append(sequence.name);
      sql.append(flavor.sequenceOptions());
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

  private void executeOrPrint(Sql sql, Database db, StringBuilder script) {
    if (db != null) {
      db.ddl(sql.toString()).execute();
    } else {
      script.append(sql.toString());
      script.append(";\n\n");
    }
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
