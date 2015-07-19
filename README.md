## Easier, Safer Database Access

[![Build Status](https://travis-ci.org/susom/database.svg?branch=master)](https://travis-ci.org/susom/database)

The point of this project is to provide a simplified way of accessing databases. It is a
wrapper around the JDBC driver, and tries to hide some of the more error-prone, unsafe, and non-portable
parts of the standard API. It uses standard Java types for all operations (as opposed to java.sql.*),
and acts as a compatibility layer in making every attempt to behave consistently
with all supported databases.

The operations supported are the typical relational database ones. This is NOT an object-relational
mapping layer or attempt to create a new language for querying.

### Features

#### No checked exceptions

All SQLExceptions are wrapped into a DatabaseException that inherits from
RuntimeException. This makes code much cleaner because in server programming there is usually no
way to recover from an error (it is handled in a generic way by higher level code).

#### No way to control (mess up) resource handling

Connections, prepared statements, result sets,
and everything else that requires `close()` calls are hidden so there is no opportunity for client
code to make a mistake and cause resource leaks.

#### Type safe with null parameters

The various `argX()` calls know the type of the object you intend to pass, and can
therefore handle null values correctly. No more errors because you pass a null and the JDBC
driver can't figure out what type it should be.

```java
  db.toInsert("insert into foo (bar) values (?)").argLong(maybeNull).insert(1);
```

#### Indexed or named parameters

You can use traditional positional parameters in the SQL (the '?' character),
or you can use named parameters. This can help reduce errors due to counting
incorrectly.

```java
  db.toUpdate("update foo set bar=?").argLong(23L).update();
  db.toUpdate("update foo set bar=:baz").argLong("baz", 23L).update();
```

You can use both positional and named within the same SQL statement. The positional
parameters must be in the correct order, but the `arg*()` calls for the named
parameters can be mixed anywhere among the positional ones.

```java
  db.toSelect("select c from t where a=:a and b=?")
      .argString("value for b")
      .argString(":a", "value for a")
      .queryLongOrNull();
```

Parameter setting can also be deferred for convenient dynamic SQL generation:

```java
  Sql sql = new Sql();

  sql.append("select a from b where c=?").argInteger(1);

  if (d) {
    sql.append(" and d=?").argString("foo");
  }

  db.toSelect(sql).query(...);
```

#### Correct handling of java.util.Date

As long as your database columns have enough precision, Date
objects will round-trip correctly with millisecond precision. No more fiddling with Timestamp
and dealing with millisecond truncation and nanoseconds.

```java
  Date now = new Date(); // java.util.Date

  db.toInsert("insert into t (pk,d) values (?,?)")
      .argInteger(123)
      .argDate(now)
      .insert(1);
  Date sameNow = db.toSelect("select d from t where pk=?")
      .argInteger(123)
      .queryDateOrNull();

  if (now.equals(sameNow)) {
    // don't look so surprised...
  }
```

There is also a convenient way to deal with "now", which hides the `new Date()` call
within the configurable `Options`. This is handy for testing because you can explicitly
control and manipulate the clock.

```java
  db.toInsert(...).argDateNowPerApp().insert();
```

Since every database seems to have a different way of dealing with time, this library also
tries to smooth out some of the syntactic (and semantic) differences in using time according
to the database server (the operating system time).

```java
  db.toInsert(...).argDateNowPerDb().insert();
```

For Oracle the above code will substitute `systimestamp(3)` for the parameter, while for PostgreSQL
the value `date_trunc('milliseconds',localtimestamp)` will be used. To make testing easier,
there is also a configuration option to make the above code do exactly the same thing as
`argDateNowPerApp()` so you can in effect control the database server time as well as that
of the application server.

#### Correct handling of java.math.BigDecimal

Similarly BigDecimal tries to maintain scale in a more intuitive manner
(most drivers will revert to "full precision" meaning they pad the scale out to what the database column specifies).

#### Simplified handling of CLOBs and BLOBs

Deal with them explicitly as either String/byte[] or streams.
No downcasting or driver-specific APIs, and treat them the same as other parameters.

#### Central control of instrumentation and logging

Tracks important metrics and logs to SLF4J in a way that is cleaner and
gives you more control than having database-related logging scattered throughout your code.
The logging is customizable so you can choose to see substituted parameters as
well.

```
Get database: 393.282ms(getConn=389.948ms,checkAutoCommit=1.056ms,dbInit=2.273ms)
DDL: 15.658ms(prep=8.017ms,exec=7.619ms,close=0.021ms) create table dbtest (a numeric)
Insert: 71.295ms(prep=65.093ms,exec=6.153ms,close=0.048ms) insert into dbtest (a) values (?)|insert into dbtest (a) values (23)
Query: 38.627ms(prep=27.642ms,exec=9.846ms,read=1.013ms,close=0.125ms) select count(1) from dbtest|select count(1) from dbtest
```

#### Allocation of Primary Keys

It is often a good idea to generate primary keys from a sequence, but this is
not always easy to do in a clean and efficient way. This library provides a
way to do it that will be efficient on advanced databases, and still be able to
transparently fall back to multiple database calls when necessary.

```java
  Long pk = db.toInsert(
      "insert into t (pk,s) values (?,?)")
      .argPkSeq("pk_seq")
      .argString("Hi")
      .insertReturningPkSeq("pk");
```

This has a more general form for returning multiple columns. For example, if you
inserted a database timestamp and need that value as well to update an object in memory:

```java
  db.toInsert("insert into t (pk,d,s) values (?,?,?)")
      .argPkSeq("pk_seq")
      .argDateNowPerDb()
      .argString("Hi")
      .insertReturning("t", "pk", new RowsHandler<Foo>() {
        @Override
        public Foo process(Rows rs) throws Exception {
          ...
          if (rs.next()) {
            ... = rs.getLongOrNull(1); // value of pk
            ... = rs.getDateOrNull(2); // value of d
          }
          ...
        }
      }, "d");
```

#### Fluent API that is auto-completion friendly

Built to make life easier in modern IDEs. Everything you need is accessed from a
single interface (Database).

Methods within the library have also been annotated to help IDEs like IntelliJ
provide better support. For example, it can warn you about checking nulls, or
forgetting to use a return value on a fluent API. Try using the
[error-prone](http://errorprone.info/) plugin and/or build tools in your project.

#### Schema Definition and Creation

You can define your database schema using a simple Java API and execute the database specific DDL.
When defining this schema you use the same basic Java types you use when querying, and appropriate
database-specific column types will be chosen such that data will round-trip correctly. This
API also smooths over some syntax differences like sequence creation.

```java
  // Observe that this will work across the supported databases, with
  // specific syntax and SQL types tuned for that database.
  new Schema()
      .addTable("t")
        .addColumn("pk").primaryKey().table()
        .addColumn("d").asDate().table()
        .addColumn("s").asString(80).table().schema()
      .addSequence("pk_seq").schema().execute(db);
```

### Quick Examples

Basic example including setup:

```java
  String url = "jdbc:derby:testdb;create=true";
  DatabaseProvider.fromDriverManager(url).transact(new DbRun() {
    @Override
    public void run(Provider<Database> dbp) {
      Database db = dbp.get();
      db.ddl("drop table t").executeQuietly();
      db.ddl("create table t (a numeric)").execute();
      db.toInsert("insert into t (a) values (?)").argInteger(32).insert(1);
      db.toUpdate("update t set a=:val").argInteger("val", 23).update();

      Long rows = db.toSelect("select count(1) from t").queryLongOrNull();
      System.out.println("Rows: " + rows);
    }
  });
```

Note the lack of error handling, resource management, and transaction calls. This
is not because it is left as an exercise for the reader, but because it is handled
automatically.

For a more realistic server-side example, a container will usually manage creation
of the Database or Provider<Database>, and business layer code will declare a 
dependency on this:

```java
public class MyBusiness {
  private Provider<Database> db;

  public MyBusiness(Provider<Database> db) {
    this.db = db;
  }

  public Long doStuff(String data) {
    if (isCached(data)) {
      // Note how this might never allocate a database connection
      return cached(data);
    }
    
    return db.get().toSelect("select count(*) from a where b=:data")
             .argString("data", data).queryLong();
  }

  // Note we use only java.util.Date, not java.sql.*
  public List<Date> doMoreStuff(Date after) {
    return db.get().toSelect("select my_date from a where b > ?").argDate(after)
           .query(new RowsHandler<List<Date>>() {
              @Override
              public List<Date> process(Rows rs) throws Exception {
                List<Date> result = new ArrayList<>();
                while (rs.next()) {
                  result.add(rs.getDateOrNull("my_date"));
                }
                return result;
              }
           });
  }
}
```

Note the handlers are Java 8 closure-friendly, so the last method above could be written:

```java
  public List<Date> doMoreStuff(Date after) {
    return db.get().toSelect("select my_date from a where b > ?").argDate(after)
           .query(rs -> {
              List<Date> result = new ArrayList<>();
              while (rs.next()) {
                result.add(rs.getDateOrNull("my_date"));
              }
              return result;
           });
  }
```

Of course there are also convenience methods for simple cases like
this having only one column in the result:

```java
  public List<Date> doMoreStuff(Date after) {
    return db.get().toSelect("select my_date from a where b > ?")
        .argDate(after).queryDates();
  }
```

### Getting Started

The library is available in the public Maven repository:

```
<dependency>
  <groupId>com.github.susom</groupId>
  <artifactId>database</artifactId>
  <version>1.3</version>
</dependency>
```

Just add that to your pom.xml, use one of the static builder methods on 
`com.github.susom.database.DatabaseProvider` (see example above), and enjoy!

To see more examples of how to use the library, take a look at
some of the tests:

[CommonTest.java](https://github.com/susom/database/blob/master/src/test/java/com/github/susom/database/test/CommonTest.java)

### Limitations

The functionality is currently tested with Oracle, Derby, and PostgreSQL. It
won't work out of the box with other databases right now, but should in the future.

The library is compiled and tested with Java 7, so it won't work with Java 6.

There is currently no support for batch inserts (hopefully coming soon).

No fancy things with results (e.g. scrolling or updating result sets).
