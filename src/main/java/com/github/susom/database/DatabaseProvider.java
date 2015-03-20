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

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.CheckReturnValue;
import javax.inject.Provider;
import javax.naming.Context;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a lazy provider for Database instances. It helps avoid allocating connection
 * or transaction resources until (or if) we actually need a Database. As a consequence
 * of this laziness, the underlying resources require explicit cleanup by calling either
 * commitAndClose() or rollbackAndClose().
 *
 * @author garricko
 */
public final class DatabaseProvider implements Provider<Database> {
  private static final Logger log = LoggerFactory.getLogger(DatabaseProvider.class);
  private DatabaseProvider delegateTo = null;
  private Provider<Connection> connectionProvider;
  private boolean txStarted = false;
  private Connection connection = null;
  private Database database = null;
  private final Options options;

  public DatabaseProvider(Provider<Connection> connectionProvider, Options options) {
    this.connectionProvider = connectionProvider;
    this.options = options;
  }

  private DatabaseProvider(DatabaseProvider delegateTo) {
    this.delegateTo = delegateTo;
    this.options = delegateTo.options;
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

    return new BuilderImpl(new Provider<Connection>() {
      @Override
      public Connection get() {
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

    return new BuilderImpl(new Provider<Connection>() {
      @Override
      public Connection get() {
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

  /**
   * This is a convenience method to eliminate the need for explicitly
   * managing the resources (and error handling) for this class.
   *
   * @param run the code you want to run as a transaction with a Database
   */
  public void transact(DbRun run) {
    if (delegateTo != null) {
      delegateTo.transact(run);
      return;
    }

    boolean complete = false;
    try {
      run.run(this);
      complete = true;
    } catch (Exception e) {
      throw new DatabaseException("Exception during transaction", e);
    } finally {
      if (run.isRollbackOnly() || (run.isRollbackOnError() && !complete)) {
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
     * Note that if you use this method you are responsible for managing
     * the transaction and commit/rollback/close.
     */
    @CheckReturnValue
    DatabaseProvider create();

    /**
     * Use this method to have resource management handled for you.
     *
     * @param run the code you want to run as a transaction with a Database
     */
    void transact(DbRun run);
  }

  private static class BuilderImpl implements Builder {
    private final Provider<Connection> connectionProvider;
    private final Options options;

    private BuilderImpl(Provider<Connection> connectionProvider, Options options) {
      this.connectionProvider = connectionProvider;
      this.options = options;
    }

    @Override
    public Builder withOptions(OptionsOverride options) {
      return new BuilderImpl(connectionProvider, options.withParent(this.options));
    }

    @Override
    public Builder withSqlParameterLogging() {
      return new BuilderImpl(connectionProvider, new OptionsOverride() {
        @Override
        public boolean isLogParameters() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withSqlInExceptionMessages() {
      return new BuilderImpl(connectionProvider, new OptionsOverride() {
        @Override
        public boolean isDetailedExceptions() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withDatePerAppOnly() {
      return new BuilderImpl(connectionProvider, new OptionsOverride() {
        @Override
        public boolean useDatePerAppOnly() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withTransactionControl() {
      return new BuilderImpl(connectionProvider, new OptionsOverride() {
        @Override
        public boolean allowTransactionControl() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withTransactionControlSilentlyIgnored() {
      return new BuilderImpl(connectionProvider, new OptionsOverride() {
        @Override
        public boolean ignoreTransactionControl() {
          return true;
        }
      }.withParent(this.options));
    }

    public DatabaseProvider create() {
      return new DatabaseProvider(connectionProvider, options);
    }

    public void transact(DbRun run) {
      create().transact(run);
    }
  }

  public Database get() {
    if (delegateTo != null) {
      return delegateTo.get();
    }

    if (database != null) {
      return database;
    }

    Metric metric = new Metric(log.isDebugEnabled());
    try {
      connection = connectionProvider.get();
      txStarted = true;
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
        StringBuilder buf = new StringBuilder("Get database: ");
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
      public DatabaseProvider create() {
        return new DatabaseProvider(DatabaseProvider.this);
      }

      @Override
      public void transact(DbRun dbRun) {
        create().transact(dbRun);
      }
    };
  }

  public void commitAndClose() {
    if (delegateTo != null) {
      log.debug("Ignoring commitAndClose() because this is a fake provider");
      return;
    }

    if (txStarted) {
      try {
        connection.commit();
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

    if (txStarted) {
      try {
        connection.rollback();
      } catch (Exception e) {
        log.error("Unable to rollback the transaction", e);
      }
      close();
    }
  }

  private void close() {
    try {
      connection.close();
    } catch (Exception e) {
      log.error("Unable to close the database connection", e);
    }
    connection = null;
    database = null;
    txStarted = false;
  }
}
