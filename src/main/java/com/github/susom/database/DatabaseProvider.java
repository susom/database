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

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.naming.Context;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

/**
 * This is a lazy provider for Database instances. It helps avoid allocating connection
 * or transaction resources until (or if) we actually need a Database. As a consequence
 * of this laziness, the underlying resources require explicit cleanup by calling either
 * commitAndClose() or rollbackAndClose().
 *
 * @author garricko
 */
public final class DatabaseProvider implements Supplier<Database> {
  private static final Logger log = LoggerFactory.getLogger(DatabaseProvider.class);
  private static final AtomicInteger poolNameCounter = new AtomicInteger(1);
  private DatabaseProvider delegateTo = null;
  private Supplier<Connection> connectionProvider;
  private Connection connection = null;
  private Database database = null;
  private final Options options;

  public DatabaseProvider(Supplier<Connection> connectionProvider, Options options) {
    if (connectionProvider == null) {
      throw new IllegalArgumentException("Connection provider cannot be null");
    }
    this.connectionProvider = connectionProvider;
    this.options = options;
  }

  private DatabaseProvider(DatabaseProvider delegateTo) {
    this.delegateTo = delegateTo;
    this.options = delegateTo.options;
  }

  /**
   * Configure the database from the following properties read from the provided configuration:
   * <br/>
   * <pre>
   *   database.url=...       Database connect string (required)
   *   database.user=...      Authenticate as this user (optional if provided in url)
   *   database.password=...  User password (optional if user and password provided in
   *                          url; prompted on standard input if user is provided and
   *                          password is not)
   *   database.pool.size=... How many connections in the connection pool (default 10).
   *   database.driver.class  The driver to initialize with Class.forName(). This will
   *                          be guessed from the database.url if not provided.
   *   database.flavor        One of the enumerated values in {@link Flavor}. If this
   *                          is not provided the flavor will be guessed based on the
   *                          value for database.url, if possible.
   * </pre>
   *
   * <p>The database flavor will be guessed based on the URL.</p>
   *
   * <p>A database pool will be created using HikariCP.</p>
   *
   * <p>Be sure to retain a copy of the builder so you can call close() later to
   * destroy the pool. You will most likely want to register a JVM shutdown hook
   * to make sure this happens. See VertxServer.java in the demo directory for
   * an example of how to do this.</p>
   */
  @CheckReturnValue
  public static Builder pooledBuilder(Config config) {
    return fromPool(createPool(config));
  }

  /**
   * Use an externally configured DataSource, Flavor, and optionally a shutdown hook.
   * The shutdown hook may be null if you don't want calls to Builder.close() to attempt
   * any shutdown. The DataSource and Flavor are mandatory.
   */
  @CheckReturnValue
  public static Builder fromPool(Pool pool) {
    return new BuilderImpl(pool.poolShutdown, () -> {
      try {
        return pool.dataSource.getConnection();
      } catch (Exception e) {
        throw new DatabaseException("Unable to obtain a connection from the DataSource", e);
      }
    }, new OptionsDefault(pool.flavor));
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method. The url parameter will be inspected
   * to determine the Flavor for this database.
   */
  @CheckReturnValue
  public static Builder fromDriverManager(String url) {
    return fromDriverManager(url, Flavor.fromJdbcUrl(url), null, null, null);
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method.
   *
   * @param flavor use this flavor rather than guessing based on the url
   */
  @CheckReturnValue
  public static Builder fromDriverManager(String url, Flavor flavor) {
    return fromDriverManager(url, flavor, null, null, null);
  }

  public static Builder fromDriverManager(Config config) {
    return fromDriverManager(config.getString("database.url"), config.getString("database.user"),
        config.getString("database.password"));
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method. The url parameter will be inspected
   * to determine the Flavor for this database.
   */
  @CheckReturnValue
  public static Builder fromDriverManager(String url, Properties info) {
    return fromDriverManager(url, Flavor.fromJdbcUrl(url), info, null, null);
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method.
   *
   * @param flavor use this flavor rather than guessing based on the url
   */
  @CheckReturnValue
  public static Builder fromDriverManager(String url, Flavor flavor, Properties info) {
    return fromDriverManager(url, flavor, info, null, null);
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method. The url parameter will be inspected
   * to determine the Flavor for this database.
   */
  @CheckReturnValue
  public static Builder fromDriverManager(String url, String user, String password) {
    return fromDriverManager(url, Flavor.fromJdbcUrl(url), null, user, password);
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method.
   *
   * @param flavor use this flavor rather than guessing based on the url
   */
  @CheckReturnValue
  public static Builder fromDriverManager(String url, Flavor flavor, String user, String password) {
    return fromDriverManager(url, flavor, null, user, password);
  }

  private static Builder fromDriverManager(final String url, Flavor flavor, final Properties info,
                                           final String user, final String password) {
    Options options = new OptionsDefault(flavor);

    // Make sure DriverManager can locate the driver
    try {
      DriverManager.getDriver(url);
    } catch (SQLException e) {
      try {
        Class.forName(Flavor.driverForJdbcUrl(url));
      } catch (ClassNotFoundException e1) {
        throw new DatabaseException("Couldn't locate JDBC driver - try setting -Djdbc.drivers=some.Driver", e1);
      }
    }

    return new BuilderImpl(null, () -> {
      try {
        if (info != null) {
          return DriverManager.getConnection(url, info);
        } else if (user != null) {
          return DriverManager.getConnection(url, user, password);
        }
        return DriverManager.getConnection(url);
      } catch (Exception e) {
        throw new DatabaseException("Unable to obtain a connection from DriverManager", e);
      }
    }, options);
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * a JNDI resource. To use this method you must explicitly indicate what
   * Flavor of database we are dealing with.
   */
  @CheckReturnValue
  public static Builder fromJndi(final Context context, final String lookupKey, Flavor flavor) {
    Options options = new OptionsDefault(flavor);

    return new BuilderImpl(null, () -> {
      DataSource ds;
      try {
        ds = (DataSource) context.lookup(lookupKey);
      } catch (Exception e) {
        throw new DatabaseException("Unable to locate the DataSource in JNDI using key " + lookupKey, e);
      }
      try {
        return ds.getConnection();
      } catch (Exception e) {
        throw new DatabaseException("Unable to obtain a connection from JNDI DataSource " + lookupKey, e);
      }
    }, options);
  }

  /**
   * Configure the database from up to five properties read from a file:
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * <p>This will use the JVM default character encoding to read the property file.</p>
   * @param filename path to the properties file we will attempt to read
   * @throws DatabaseException if the property file could not be read for any reason
   */
  public static Builder fromPropertyFile(String filename) {
    return fromPropertyFile(filename, Charset.defaultCharset().newDecoder());
  }

  /**
   * Configure the database from up to five properties read from a file:
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * @param filename path to the properties file we will attempt to read
   * @param decoder  character encoding to use when reading the property file
   * @throws DatabaseException if the property file could not be read for any reason
   */
  public static Builder fromPropertyFile(String filename, CharsetDecoder decoder) {
    Properties properties = new Properties();
    if (filename != null && filename.length() > 0) {
      try {
        properties.load(new InputStreamReader(new FileInputStream(filename), decoder));
      } catch (Exception e) {
        throw new DatabaseException("Unable to read properties file: " + filename, e);
      }
    }
    return fromProperties(properties, "", true);
  }

  /**
   * Configure the database from up to five properties read from a file:
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * <p>This will use the JVM default character encoding to read the property file.</p>
   * @param filename path to the properties file we will attempt to read
   * @param propertyPrefix if this is null or empty the properties above will be read;
   *                       if a value is provided it will be prefixed to each property
   *                       (exactly, so if you want to use "my.database.url" you must
   *                       pass "my." as the prefix)
   * @throws DatabaseException if the property file could not be read for any reason
   */
  public static Builder fromPropertyFile(String filename, String propertyPrefix) {
    return fromPropertyFile(filename, propertyPrefix, Charset.defaultCharset().newDecoder());
  }

  /**
   * Configure the database from up to five properties read from a file:
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * @param filename path to the properties file we will attempt to read
   * @param propertyPrefix if this is null or empty the properties above will be read;
   *                       if a value is provided it will be prefixed to each property
   *                       (exactly, so if you want to use "my.database.url" you must
   *                       pass "my." as the prefix)
   * @param decoder  character encoding to use when reading the property file
   * @throws DatabaseException if the property file could not be read for any reason
   */
  public static Builder fromPropertyFile(String filename, String propertyPrefix, CharsetDecoder decoder) {
    Properties properties = new Properties();
    if (filename != null && filename.length() > 0) {
      try {
        properties.load(new InputStreamReader(new FileInputStream(filename), decoder));
      } catch (Exception e) {
        throw new DatabaseException("Unable to read properties file: " + filename, e);
      }
    }
    return fromProperties(properties, propertyPrefix, true);
  }

  /**
   * Configure the database from up to five properties read from the provided properties:
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * @param properties properties will be read from here
   * @throws DatabaseException if the property file could not be read for any reason
   */
  public static Builder fromProperties(Properties properties) {
    return fromProperties(properties, "", false);
  }

  /**
   * Configure the database from up to five properties read from the provided properties:
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * @param properties properties will be read from here
   * @param propertyPrefix if this is null or empty the properties above will be read;
   *                       if a value is provided it will be prefixed to each property
   *                       (exactly, so if you want to use "my.database.url" you must
   *                       pass "my." as the prefix)
   * @throws DatabaseException if the property file could not be read for any reason
   */
  public static Builder fromProperties(Properties properties, String propertyPrefix) {
    return fromProperties(properties, propertyPrefix, false);
  }

  /**
   * Configure the database from up to five properties read from the specified
   * properties file, or from the system properties (system properties will take
   * precedence over the file):
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * <p>This will use the JVM default character encoding to read the property file.</p>
   * @param filename path to the properties file we will attempt to read; if the file
   *                 cannot be read for any reason (e.g. does not exist) a debug level
   *                 log entry will be entered, but it will attempt to proceed using
   *                 solely the system properties
   */
  public static Builder fromPropertyFileOrSystemProperties(String filename) {
    return fromPropertyFileOrSystemProperties(filename, Charset.defaultCharset().newDecoder());
  }

  /**
   * Configure the database from up to five properties read from the specified
   * properties file, or from the system properties (system properties will take
   * precedence over the file):
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * @param filename path to the properties file we will attempt to read; if the file
   *                 cannot be read for any reason (e.g. does not exist) a debug level
   *                 log entry will be entered, but it will attempt to proceed using
   *                 solely the system properties
   * @param decoder  character encoding to use when reading the property file
   */
  public static Builder fromPropertyFileOrSystemProperties(String filename, CharsetDecoder decoder) {
    Properties properties = new Properties();
    if (filename != null && filename.length() > 0) {
      try {
        properties.load(new InputStreamReader(new FileInputStream(filename), decoder));
      } catch (Exception e) {
        log.debug("Trying system properties - unable to read properties file: " + filename);
      }
    }
    return fromProperties(properties, "", true);
  }

  /**
   * Configure the database from up to five properties read from the specified
   * properties file, or from the system properties (system properties will take
   * precedence over the file):
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * <p>This will use the JVM default character encoding to read the property file.</p>
   * @param filename path to the properties file we will attempt to read; if the file
   *                 cannot be read for any reason (e.g. does not exist) a debug level
   *                 log entry will be entered, but it will attempt to proceed using
   *                 solely the system properties
   * @param propertyPrefix if this is null or empty the properties above will be read;
   *                       if a value is provided it will be prefixed to each property
   *                       (exactly, so if you want to use "my.database.url" you must
   *                       pass "my." as the prefix)
   */
  public static Builder fromPropertyFileOrSystemProperties(String filename, String propertyPrefix) {
    return fromPropertyFileOrSystemProperties(filename, propertyPrefix, Charset.defaultCharset().newDecoder());
  }

  /**
   * Configure the database from up to five properties read from the specified
   * properties file, or from the system properties (system properties will take
   * precedence over the file):
   * <br/>
   * <pre>
   *   database.url=...      Database connect string (required)
   *   database.user=...     Authenticate as this user (optional if provided in url)
   *   database.password=... User password (optional if user and password provided in
   *                         url; prompted on standard input if user is provided and
   *                         password is not)
   *   database.flavor=...   What kind of database it is (optional, will guess based
   *                         on the url if this is not provided)
   *   database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                         guess based on the flavor if this is not provided)
   * </pre>
   * @param filename path to the properties file we will attempt to read; if the file
   *                 cannot be read for any reason (e.g. does not exist) a debug level
   *                 log entry will be entered, but it will attempt to proceed using
   *                 solely the system properties
   * @param propertyPrefix if this is null or empty the properties above will be read;
   *                       if a value is provided it will be prefixed to each property
   *                       (exactly, so if you want to use "my.database.url" you must
   *                       pass "my." as the prefix)
   * @param decoder  character encoding to use when reading the property file
   */
  public static Builder fromPropertyFileOrSystemProperties(String filename, String propertyPrefix,
                                                           CharsetDecoder decoder) {
    Properties properties = new Properties();
    if (filename != null && filename.length() > 0) {
      try {
        properties.load(new InputStreamReader(new FileInputStream(filename), decoder));
      } catch (Exception e) {
        log.debug("Trying system properties - unable to read properties file: " + filename);
      }
    }
    return fromProperties(properties, propertyPrefix, true);
  }

  /**
   * Configure the database from up to five system properties:
   * <br/>
   * <pre>
   *   -Ddatabase.url=...      Database connect string (required)
   *   -Ddatabase.user=...     Authenticate as this user (optional if provided in url)
   *   -Ddatabase.password=... User password (optional if user and password provided in
   *                           url; prompted on standard input if user is provided and
   *                           password is not)
   *   -Ddatabase.flavor=...   What kind of database it is (optional, will guess based
   *                           on the url if this is not provided)
   *   -Ddatabase.driver=...   The Java class of the JDBC driver to load (optional, will
   *                           guess based on the flavor if this is not provided)
   * </pre>
   */
  @CheckReturnValue
  public static Builder fromSystemProperties() {
    return fromProperties(null, "", true);
  }

  /**
   * Configure the database from up to five system properties:
   * <br/>
   * <pre>
   *   -D{prefix}database.url=...      Database connect string (required)
   *   -D{prefix}database.user=...     Authenticate as this user (optional if provided in url)
   *   -D{prefix}database.password=... User password (optional if user and password provided in
   *                                   url; prompted on standard input if user is provided and
   *                                   password is not)
   *   -D{prefix}database.flavor=...   What kind of database it is (optional, will guess based
   *                                   on the url if this is not provided)
   *   -D{prefix}database.driver=...   The Java class of the JDBC driver to load (optional, will
   *                                   guess based on the flavor if this is not provided)
   * </pre>
   * @param propertyPrefix a prefix to attach to each system property - be sure to include the
   *                       dot if desired (e.g. "mydb." for properties like -Dmydb.database.url)
   */
  @CheckReturnValue
  public static Builder fromSystemProperties(String propertyPrefix) {
    return fromProperties(null, propertyPrefix, true);
  }

  private static Builder fromProperties(Properties properties, String propertyPrefix, boolean useSystemProperties) {
    if (propertyPrefix == null) {
      propertyPrefix = "";
    }

    String driver;
    String flavorStr;
    String url;
    String user;
    String password;
    if (useSystemProperties) {
      if (properties == null) {
        properties = new Properties();
      }
      driver = System.getProperty(propertyPrefix + "database.driver",
          properties.getProperty(propertyPrefix + "database.driver"));
      flavorStr = System.getProperty(propertyPrefix + "database.flavor",
          properties.getProperty(propertyPrefix + "database.flavor"));
      url = System.getProperty(propertyPrefix + "database.url",
          properties.getProperty(propertyPrefix + "database.url"));
      user = System.getProperty(propertyPrefix + "database.user",
          properties.getProperty(propertyPrefix + "database.user"));
      password = System.getProperty(propertyPrefix + "database.password",
          properties.getProperty(propertyPrefix + "database.password"));
    } else {
      if (properties == null) {
        throw new DatabaseException("No properties were provided");
      }
      driver = properties.getProperty(propertyPrefix + "database.driver");
      flavorStr = properties.getProperty(propertyPrefix + "database.flavor");
      url = properties.getProperty(propertyPrefix + "database.url");
      user = properties.getProperty(propertyPrefix + "database.user");
      password = properties.getProperty(propertyPrefix + "database.password");
    }

    if (url == null) {
      throw new DatabaseException("You must use -D" + propertyPrefix + "database.url=...");
    }

    if (user != null && password == null) {
      System.out.println("Enter database password for user " + user + ":");
      byte[] input = new byte[256];
      try {
        int bytesRead = System.in.read(input);
        password = new String(input, 0, bytesRead-1, Charset.defaultCharset());
      } catch (IOException e) {
        throw new DatabaseException("Error reading password from standard input", e);
      }
    }

    Flavor flavor;
    if (flavorStr != null) {
      flavor = Flavor.valueOf(flavorStr);
    } else {
      flavor = Flavor.fromJdbcUrl(url);
    }

    if (driver == null) {
      if (flavor == Flavor.oracle) {
        driver = "oracle.jdbc.OracleDriver";
      } else if (flavor == Flavor.postgresql) {
        driver = "org.postgresql.Driver";
      } else if (flavor == Flavor.derby) {
        driver = "org.apache.derby.jdbc.EmbeddedDriver";
      }
    }
    if (driver != null) {
      try {
        Class.forName(driver).newInstance();
      } catch (Exception e) {
        throw new DatabaseException("Unable to load JDBC driver: " + driver, e);
      }
    }

    if (user == null) {
      return fromDriverManager(url, flavor);
    } else {
      return fromDriverManager(url, flavor, user, password);
    }
  }

  public void transact(final DbCode code) {
    boolean complete = false;
    try {
      code.run(this);
      complete = true;
    } catch (ThreadDeath|DatabaseException t) {
      throw t;
    } catch (Throwable t) {
      throw new DatabaseException("Error during transaction", t);
    } finally {
      if (!complete) {
        rollbackAndClose();
      } else {
        commitAndClose();
      }
    }
  }

  public <T> T transactReturning(final DbCodeTyped<T> code) {
    T result;
    boolean complete = false;
    try {
      result = code.run(this);
      complete = true;
    } catch (ThreadDeath|DatabaseException t) {
      throw t;
    } catch (Throwable t) {
      throw new DatabaseException("Error during transaction", t);
    } finally {
      if (!complete) {
        rollbackAndClose();
      } else {
        commitAndClose();
      }
    }
    return result;
  }

  public void transact(final DbCodeTx code) {
    Transaction tx = new TransactionImpl();
    tx.setRollbackOnError(true);
    tx.setRollbackOnly(false);
    boolean complete = false;
    try {
      code.run(this, tx);
      complete = true;
    } catch (ThreadDeath|DatabaseException t) {
      throw t;
    } catch (Throwable t) {
      throw new DatabaseException("Error during transaction", t);
    } finally {
      if ((!complete && tx.isRollbackOnError()) || tx.isRollbackOnly()) {
        rollbackAndClose();
      } else {
        commitAndClose();
      }
    }
  }

  /**
   * This builder is immutable, so setting various options does not affect
   * the previous instance. This is intended to make it safe to pass builders
   * around without risk someone will reconfigure it.
   */
  public interface Builder {
    @CheckReturnValue
    Builder withOptions(OptionsOverride options);

    /**
     * Enable logging of parameter values along with the SQL.
     */
    @CheckReturnValue
    Builder withSqlParameterLogging();

    /**
     * Include SQL in exception messages. This will also include parameters in the
     * exception messages if SQL parameter logging is enabled. This is handy for
     * development, but be careful as this is an information disclosure risk,
     * dependent on how the exception are caught and handled.
     */
    @CheckReturnValue
    Builder withSqlInExceptionMessages();

    /**
     * Wherever argDateNowPerDb() is specified, use argDateNowPerApp() instead. This is
     * useful for testing purposes as you can use OptionsOverride to provide your
     * own system clock that will be used for time travel.
     */
    @CheckReturnValue
    Builder withDatePerAppOnly();

    /**
     * Allow provided Database instances to explicitly control transactions using the
     * commitNow() and rollbackNow() methods. Otherwise calling those methods would
     * throw an exception.
     */
    @CheckReturnValue
    Builder withTransactionControl();

    /**
     * This can be useful when testing code, as it can pretend to use transactions,
     * while giving you control over whether it actually commits or rolls back.
     */
    @CheckReturnValue
    Builder withTransactionControlSilentlyIgnored();

    /**
     * Allow direct access to the underlying database connection. Normally this is
     * not allowed, and is a bad idea, but it can be helpful when migrating from
     * legacy code that works with raw JDBC.
     */
    @CheckReturnValue
    Builder withConnectionAccess();

    /**
     * WARNING: You should try to avoid using this method. If you use it more
     * that once or twice in your entire codebase you are probably doing
     * something wrong.
     *
     * <p>If you use this method you are responsible for managing
     * the transaction and commit/rollback/close.</p>
     */
    @CheckReturnValue
    DatabaseProvider create();

    /**
     * This is a convenience method to eliminate the need for explicitly
     * managing the resources (and error handling) for this class. After
     * the run block is complete the transaction will commit unless the
     * {@link DbCode#run(Supplier) run(Supplier)} method threw a {@link Throwable}.
     *
     * <p>Here is a typical usage:
     * <pre>
     *   dbp.transact(dbs -> {
     *     List<String> r = dbs.get().toSelect("select a from b where c=?").argInteger(1).queryStrings();
     *     ... do something with the results ...
     *   });
     * </pre>
     * </p>
     *
     * @param code the code you want to run as a transaction with a Database
     * @see #transact(DbCodeTx)
     */
    void transact(DbCode code);

    /**
     * This method is the same as {@link #transact(DbCode)} but allows a return value.
     *
     * <p>Here is a typical usage:
     * <pre>
     *   List<String> r = dbp.transact(dbs -> {
     *     return dbs.get().toSelect("select a from b where c=?").argInteger(1).queryStrings();
     *   });
     * </pre>
     * </p>
     */
    <T> T transactReturning(DbCodeTyped<T> code);

    /**
     * This is a convenience method to eliminate the need for explicitly
     * managing the resources (and error handling) for this class. After
     * the run block is complete commit() will be called unless either the
     * {@link DbCodeTx#run(Supplier, Transaction)} method threw a {@link Throwable}
     * while {@link Transaction#isRollbackOnError()} returns true, or
     * {@link Transaction#isRollbackOnly()} returns a true value.
     *
     * <p>Here is a typical usage:
     * <pre>
     *   dbp.transact((dbs, tx) -> {
     *     tx.setRollbackOnError(false);
     *     dbs.get().toInsert("...").argInteger(1).insert(1);
     *     ...some stuff that might fail...
     *   });
     * </pre>
     * </p>
     *
     * @param code the code you want to run as a transaction with a Database
     */
    void transact(DbCodeTx code);

    void close();
  }

  private static class BuilderImpl implements Builder {
    private Closeable pool;
    private final Supplier<Connection> connectionProvider;
    private final Options options;

    private BuilderImpl(Closeable pool, Supplier<Connection> connectionProvider, Options options) {
      this.pool = pool;
      this.connectionProvider = connectionProvider;
      this.options = options;
    }

    @Override
    public Builder withOptions(OptionsOverride options) {
      return new BuilderImpl(pool, connectionProvider, options.withParent(this.options));
    }

    @Override
    public Builder withSqlParameterLogging() {
      return new BuilderImpl(pool, connectionProvider, new OptionsOverride() {
        @Override
        public boolean isLogParameters() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withSqlInExceptionMessages() {
      return new BuilderImpl(pool, connectionProvider, new OptionsOverride() {
        @Override
        public boolean isDetailedExceptions() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withDatePerAppOnly() {
      return new BuilderImpl(pool, connectionProvider, new OptionsOverride() {
        @Override
        public boolean useDatePerAppOnly() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withTransactionControl() {
      return new BuilderImpl(pool, connectionProvider, new OptionsOverride() {
        @Override
        public boolean allowTransactionControl() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withTransactionControlSilentlyIgnored() {
      return new BuilderImpl(pool, connectionProvider, new OptionsOverride() {
        @Override
        public boolean ignoreTransactionControl() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withConnectionAccess() {
      return new BuilderImpl(pool, connectionProvider, new OptionsOverride() {
        @Override
        public boolean allowConnectionAccess() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public DatabaseProvider create() {
      return new DatabaseProvider(connectionProvider, options);
    }

    @Override
    public void transact(DbCode tx) {
      create().transact(tx);
    }

    @Override
    public <T> T transactReturning(DbCodeTyped<T> tx) {
      return create().transactReturning(tx);
    }

    @Override
    public void transact(DbCodeTx tx) {
      create().transact(tx);
    }

    public void close() {
      if (pool != null) {
        try {
          pool.close();
        } catch (IOException e) {
          log.warn("Unable to close connection pool", e);
        }
        pool = null;
      }
    }
  }

  public Database get() {
    if (delegateTo != null) {
      return delegateTo.get();
    }

    if (database != null) {
      return database;
    }

    if (connectionProvider == null) {
      throw new DatabaseException("Called get() on a DatabaseProvider after close()");
    }

    Metric metric = new Metric(log.isDebugEnabled());
    try {
      connection = connectionProvider.get();
      metric.checkpoint("getConn");
      try {
        // Generally check autocommit before setting because databases like
        // Oracle can get grumpy if you change it (depending on how your connection
        // has been initialized by say JNDI), but PostgresSQL seems to
        // require calling setAutoCommit() every time
        // Commenting as the Oracle 12.1.0.2 driver now seems to require this as well
        // (the getAutoCommit() call here will return false and then it will blow up
        // on commit complaining you can't commit with autocommit on)
//        if (options.flavor() == Flavor.postgresql || !connection.getAutoCommit()) {
          if (!options.flavor().autoCommitOnly())
            connection.setAutoCommit(false);
          metric.checkpoint("setAutoCommit");
//        } else {
//          metric.checkpoint("checkAutoCommit");
//        }
      } catch (SQLException e) {
        throw new DatabaseException("Unable to check/set autoCommit for the connection", e);
      }
      database = new DatabaseImpl(connection, options);
      metric.checkpoint("dbInit");
    } catch (RuntimeException e) {
      metric.checkpoint("fail");
      throw e;
    } finally {
      metric.done();
      if (log.isDebugEnabled()) {
        StringBuilder buf = new StringBuilder("Get ").append(options.flavor()).append(" database: ");
        metric.printMessage(buf);
        log.debug(buf.toString());
      }
    }
    return database;
  }


  public Builder fakeBuilder() {
    return new Builder() {
      @Override
      public Builder withOptions(OptionsOverride optionsOverride) {
        return this;
      }

      @Override
      public Builder withSqlParameterLogging() {
        return this;
      }

      @Override
      public Builder withSqlInExceptionMessages() {
        return this;
      }

      @Override
      public Builder withDatePerAppOnly() {
        return this;
      }

      @Override
      public Builder withTransactionControl() {
        return this;
      }

      @Override
      public Builder withTransactionControlSilentlyIgnored() {
        return this;
      }

      @Override
      public Builder withConnectionAccess() {
        return this;
      }

      @Override
      public DatabaseProvider create() {
        return new DatabaseProvider(DatabaseProvider.this);
      }

      @Override
      public void transact(DbCode tx) {
        create().transact(tx);
      }

      @Override
      public <T> T transactReturning(DbCodeTyped<T> tx) {
        return create().transactReturning(tx);
      }

      @Override
      public void transact(DbCodeTx tx) {
        create().transact(tx);
      }

      @Override
      public void close() {
        log.debug("Ignoring close call on fakeBuilder");
      }
    };
  }

  public void commitAndClose() {
    if (delegateTo != null) {
      log.debug("Ignoring commitAndClose() because this is a fake provider");
      return;
    }

    if (connection != null) {
      try {
        if (!options.flavor().autoCommitOnly()) {
          connection.commit();
        }
      } catch (Exception e) {
        throw new DatabaseException("Unable to commit the transaction", e);
      }
      close();
    }
  }

  public void rollbackAndClose() {
    if (delegateTo != null) {
      log.debug("Ignoring rollbackAndClose() because this is a fake provider");
      return;
    }

    if (connection != null) {
      try {
        if (options.flavor().autoCommitOnly()) {
          throw new UnsupportedOperationException("rollback is not supported");
        }
        connection.rollback();
      } catch (Exception e) {
        log.error("Unable to rollback the transaction", e);
      }
      close();
    }
  }

  private void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        log.error("Unable to close the database connection", e);
      }
    }
    connection = null;
    database = null;
    connectionProvider = null;
  }

  public static class Pool {
    public DataSource dataSource;
    public int size;
    public Flavor flavor;
    public Closeable poolShutdown;

    public Pool(DataSource dataSource, int size, Flavor flavor, Closeable poolShutdown) {
      this.dataSource = dataSource;
      this.size = size;
      this.flavor = flavor;
      this.poolShutdown = poolShutdown;
    }
  }

  public static Pool createPool(Config config) {
    String url = config.getString("database.url");
    if (url == null) {
      throw new DatabaseException("You must provide database.url");
    }

    HikariDataSource ds = new HikariDataSource();
    // If we don't provide a pool name it will automatically generate one, but
    // the way it does that requires PropertyPermission("*", "read,write") and
    // will fail if the security sandbox is enabled
    ds.setPoolName(config.getString("database.pool.name", "HikariPool-" + poolNameCounter.getAndAdd(1)));
    ds.setJdbcUrl(url);
    String driverClassName = config.getString("database.driver.class", Flavor.driverForJdbcUrl(url));
    ds.setDriverClassName(driverClassName);
    ds.setUsername(config.getString("database.user"));
    ds.setPassword(config.getString("database.password"));
    int poolSize = config.getInteger("database.pool.size", 10);
    ds.setMaximumPoolSize(poolSize);
    ds.setAutoCommit(false);

    Flavor flavor;
    String flavorString = config.getString("database.flavor");
    if (flavorString != null) {
      flavor = Flavor.valueOf(flavorString);
    } else {
      flavor = Flavor.fromJdbcUrl(url);
    }

    log.debug("Created '" + flavor + "' connection pool of size " + poolSize + " using driver " + driverClassName);

    return new Pool(ds, poolSize, flavor, ds);
  }
}
