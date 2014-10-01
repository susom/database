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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
  public static Builder fromDriverManager(String url) {
    return fromDriverManager(url, null, null, null);
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method. The url parameter will be inspected
   * to determine the Flavor for this database.
   */
  public static Builder fromDriverManager(String url, Properties info) {
    return fromDriverManager(url, info, null, null);
  }

  /**
   * Builder method to create and initialize an instance of this class using
   * the JDBC standard DriverManager method. The url parameter will be inspected
   * to determine the Flavor for this database.
   */
  public static Builder fromDriverManager(String url, String user, String password) {
    return fromDriverManager(url, null, user, password);
  }

  private static Builder fromDriverManager(final String url, final Properties info, final String user, final String password) {
    Flavor flavor = Flavor.fromJdbcUrl(url);
    Options options = new OptionsDefault(flavor);

    if (flavor == Flavor.generic) {
      log.info("Couldn't determine database flavor from JDBC URL, defaulting to generic");
    }

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
      run.run(get());
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

  public interface Builder {
    Builder withOptions(OptionsOverride options);

    /**
     * Enable logging of parameter values along with the SQL.
     */
    Builder withSqlParameterLogging();

    /**
     * Include SQL in exception messages. This will also include parameters in the
     * exception messages if SQL parameter logging is enabled. This is handy for
     * development, but be careful as this is an information disclosure risk,
     * dependent on how the exception are caught and handled.
     */
    Builder withSqlInExceptionMessages();

    /**
     * Wherever argDateNowPerDb() is specified, use argDateNowPerApp() instead. This is
     * useful for testing purposes as you can use OptionsOverride to provide your
     * own system clock that will be used for time travel.
     */
    Builder withDatePerAppOnly();

    /**
     * Allow provided Database instances to explicitly control transactions using the
     * commitNow() and rollbackNow() methods. Otherwise calling those methods would
     * throw an exception.
     */
    Builder withTransactionControl();

    /**
     * This can be useful when testing code, as it can pretend to use transactions,
     * while giving you control over whether it actually commits or rolls back.
     */
    Builder withTransactionControlSilentlyIgnored();

    /**
     * Note that if you use this method you are responsible for managing
     * the transaction and commit/rollback/close.
     */
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