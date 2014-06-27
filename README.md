## Easier, Safer JDBC

[![Build Status](https://travis-ci.org/susom/database.svg?branch=master)](https://travis-ci.org/susom/database)

The point of this project is to provide a simplified way of accessing databases. It is a
wrapper around the JDBC driver, and tries to hide some of the more error-prone and unsafe
parts of the standard API.

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
  db.insert("insert into foo (bar) values (?)").argLong(maybeNull).insert(1);
```

#### Indexed or named parameters

You can use traditional positional parameters in the SQL (the '?' character),
or you can use named parameters. This can help reduce errors due to counting
incorrectly. Note you cannot use both positional and named within the same SQL statement.

```java
  db.update("update foo set bar=?").argLong(23L).update();
  db.update("update foo set bar=:baz").argLong("baz", 23L).update();
```

#### Correct handling of java.util.Date

As long as your database columns have enough precision, Date
objects will round-trip correctly with millisecond precision. No more fiddling with Timestamp
and dealing with millisecond truncation and nanoseconds.

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

#### Fluent API that is auto-completion friendly

Built to make life easier in modern IDEs. Everything you need is accessed from a
single interface (Database).

### A Quick Example

For the impatient:

```java
  String url = "jdbc:derby:testdb;create=true";
  DatabaseProvider.fromDriverManager(url).transact(new DbRun() {
    @Override
    public void run(Database db) {
      db.ddl("drop table t").executeQuietly();
      db.ddl("create table t (a numeric)").execute();
      db.insert("insert into t (a) values (?)").argInteger(32).insert(1);
      db.update("update t set a=:val").argInteger("val", 23).update();

      Long rows = db.select("select count(1) from t").queryLong();
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
    
    return db.get().select("select count(*) from a where b=:data")
             .argString("data", data).queryLong();
  }

  // Note we use only java.util.Date, not java.sql.*
  public List<Date> doMoreStuff(Date after) {
    return db.get().select("select my_date from a where b > ?").argDate(after)
           .query(new RowsHandler<List<Date>>() {
              @Override
              public List<Date> process(Rows rs) throws Exception {
                List<Date> result = new ArrayList<>();
                while (rs.next()) {
                  result.add(rs.getDate("my_date"));
                }
                return result;
              }
           }
  }
}
```

### Getting Started

The library is available in the public Maven repository:

```
<dependency>
  <groupId>com.github.susom</groupId>
  <artifactId>database</artifactId>
  <version>1.0</version>
</dependency>
```

Just add that to your pom.xml, use one of the static builder methods on 
`com.github.susom.database.DatabaseProvider` (see example above), and enjoy!

### Limitations

The functionality is currently tested with Oracle, Derby, and PostgreSQL. It
probably won't work out of the box with other databases right now.

The library is compiled and tested with Java 7, so it won't work with Java 6.