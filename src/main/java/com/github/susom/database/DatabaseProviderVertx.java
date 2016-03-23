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
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.zaxxer.hikari.HikariDataSource;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * This is a lazy provider for Database instances. It helps avoid allocating connection
 * or transaction resources until (or if) we actually need a Database. As a consequence
 * of this laziness, the underlying resources require explicit cleanup by calling either
 * commitAndClose() or rollbackAndClose().
 *
 * @author garricko
 */
public final class DatabaseProviderVertx implements Supplier<Database> {
  private static final Logger log = LoggerFactory.getLogger(DatabaseProviderVertx.class);
  private DatabaseProviderVertx delegateTo = null;
  private Vertx vertx;
  private Provider<Connection> connectionProvider;
  private boolean txStarted = false;
  private Connection connection = null;
  private Database database = null;
  private final Options options;

  public DatabaseProviderVertx(Vertx vertx, Provider<Connection> connectionProvider, Options options) {
    if (vertx == null) {
      throw new IllegalArgumentException("Vertx cannot be null");
    }
    if (connectionProvider == null) {
      throw new IllegalArgumentException("Connection provider cannot be null");
    }
    this.vertx = vertx;
    this.connectionProvider = connectionProvider;
    this.options = options;
  }

  private DatabaseProviderVertx(DatabaseProviderVertx delegateTo) {
    this.delegateTo = delegateTo;
    this.vertx = delegateTo.vertx;
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
   * </pre>
   * <p>The database flavor will be guessed based on the URL.</p>
   * <p>A database pool will be created using HikariCP.</p>
   */
  @CheckReturnValue
  public static Builder builder(Vertx vertx, Config config) {
    String url = config.getString("database.url");
    if (url == null) {
      throw new DatabaseException("You must provide database.url");
    }
    Options options = new OptionsDefault(Flavor.fromJdbcUrl(url));
    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(url);
    ds.setDriverClassName(config.getString("database.driver.class", Flavor.driverForJdbcUrl(url)));
    ds.setUsername(config.getString("database.user"));
    ds.setPassword(config.getString("database.password"));
    ds.setMaximumPoolSize(config.getInteger("database.pool.size", 10));
    ds.setAutoCommit(false);

    return new BuilderImpl(vertx, () -> {
      try {
        return ds.getConnection();
      } catch (Exception e) {
        throw new DatabaseException("Unable to obtain a connection from DriverManager", e);
      }
    }, options);
  }

  /**
   * Execute a transaction on the current thread, with default semantics (commit if
   * the code completes successfully, or rollback if it throws an error). Note you
   * will get a DatabaseException if you try to call this from the event loop thread.
   * If the provided code block throws anything, it will be wrapped into a DatabaseException
   * unless it was a ThreadDeath or was already a DatabaseException.
   */
  public void transact(final DbCode code) {
    if (io.vertx.core.Context.isOnEventLoopThread()) {
      throw new DatabaseException("Do not call transact() from event loop threads; use transactAsync() instead");
    }

    boolean complete = false;
    try {
      code.run(this);
      complete = true;
    } catch (ThreadDeath | DatabaseException t) {
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

  /**
   * Execute a transaction on a Vert.x worker thread, with default semantics (commit if
   * the code completes successfully, or rollback if it throws an error). The provided
   * result handler will be call after the commit or rollback, and will run on the event
   * loop thread (the same thread that is calling this method).
   */
  public <T> void transactAsync(final DbCodeTyped<T> code, Handler<AsyncResult<T>> resultHandler) {
    Map mdc = MDC.getCopyOfContextMap();
    vertx.<T>executeBlocking(future -> {
      try {
        T returnValue = null;
        boolean complete = false;
        try {
          if (mdc != null) {
            MDC.setContextMap(mdc);
          }
          returnValue = code.run(this);
          complete = true;
        } catch (ThreadDeath | DatabaseException t) {
          throw t;
        } catch (Throwable t) {
          throw new DatabaseException("Error during transaction", t);
        } finally {
          if (!complete) {
            rollbackAndClose();
          } else {
            commitAndClose();
          }
          MDC.clear();
        }
        future.complete(returnValue);
      } catch (ThreadDeath t) {
        throw t;
      } catch (Throwable t) {
        future.fail(t);
      }
    }, h -> {
      try {
        if (mdc != null) {
          MDC.setContextMap(mdc);
        }
        resultHandler.handle(h);
      } finally {
        MDC.clear();
      }
    });
  }

  /**
   * Same as {@link #transact(DbCode)}, but your code block can explicitly
   * manage the behavior of the transaction.
   */
  public void transact(final DbCodeTx code) {
    if (io.vertx.core.Context.isOnEventLoopThread()) {
      throw new DatabaseException("Do not call transact() from event loop threads; use transactAsync() instead");
    }

    Transaction tx = new TransactionImpl();
    tx.setRollbackOnError(true);
    tx.setRollbackOnly(false);
    boolean complete = false;
    try {
      code.run(this, tx);
      complete = true;
    } catch (ThreadDeath | DatabaseException t) {
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
   * Execute a transaction on a Vert.x worker thread, with default semantics (commit if
   * the code completes successfully, or rollback if it throws an error). The provided
   * result handler will be call after the commit or rollback, and will run on the event
   * loop thread (the same thread that is calling this method).
   */
  public <T> void transactAsync(final DbCodeTypedTx<T> code, Handler<AsyncResult<T>> resultHandler) {
    Map mdc = MDC.getCopyOfContextMap();
    vertx.<T>executeBlocking(future -> {
      try {
        T returnValue = null;
        Transaction tx = new TransactionImpl();
        tx.setRollbackOnError(true);
        tx.setRollbackOnly(false);
        boolean complete = false;
        try {
          if (mdc != null) {
            MDC.setContextMap(mdc);
          }
          returnValue = code.run(this, tx);
          complete = true;
        } catch (ThreadDeath | DatabaseException t) {
          throw t;
        } catch (Throwable t) {
          throw new DatabaseException("Error during transaction", t);
        } finally {
          if ((!complete && tx.isRollbackOnError()) || tx.isRollbackOnly()) {
            rollbackAndClose();
          } else {
            commitAndClose();
          }
          MDC.clear();
        }
        future.complete(returnValue);
      } catch (ThreadDeath t) {
        throw t;
      } catch (Throwable t) {
        future.fail(t);
      }
    }, h -> {
      try {
        if (mdc != null) {
          MDC.setContextMap(mdc);
        }
        resultHandler.handle(h);
      } finally {
        MDC.clear();
      }
    });
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
    DatabaseProviderVertx create();

    /**
     * This is a convenience method to eliminate the need for explicitly
     * managing the resources (and error handling) for this class. After
     * the run block is complete commit() will be called unless either the
     * {@link DbCode#run(Supplier)} method threw a {@link Throwable}.
     *
     * @param code the code you want to run as a transaction with a Database
     * @see {@link #transact(DbCodeTx)} if you want to explicitly manage
     *      when the transaction commits or rolls back
     */
    void transact(DbCode code);

    <T> void transactAsync(DbCodeTyped<T> code, Handler<AsyncResult<T>> resultHandler);

    /**
     * This is a convenience method to eliminate the need for explicitly
     * managing the resources (and error handling) for this class. After
     * the run block is complete commit() will be called unless either the
     * {@link DbCodeTx#run(Supplier, Transaction)} method threw a {@link Throwable}
     * while {@link Transaction#isRollbackOnError()} returns true, or
     * {@link Transaction#isRollbackOnly()} returns a true value.
     *
     * @param code the code you want to run as a transaction with a Database
     */
    void transact(DbCodeTx code);

    <T> void transactAsync(DbCodeTypedTx<T> code, Handler<AsyncResult<T>> resultHandler);
  }

  private static class BuilderImpl implements Builder {
    private final Vertx vertx;
    private final Provider<Connection> connectionProvider;
    private final Options options;

    private BuilderImpl(Vertx vertx, Provider<Connection> connectionProvider, Options options) {
      this.vertx = vertx;
      this.connectionProvider = connectionProvider;
      this.options = options;
    }

    @Override
    public Builder withOptions(OptionsOverride options) {
      return new BuilderImpl(vertx, connectionProvider, options.withParent(this.options));
    }

    @Override
    public Builder withSqlParameterLogging() {
      return new BuilderImpl(vertx, connectionProvider, new OptionsOverride() {
        @Override
        public boolean isLogParameters() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withSqlInExceptionMessages() {
      return new BuilderImpl(vertx, connectionProvider, new OptionsOverride() {
        @Override
        public boolean isDetailedExceptions() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withDatePerAppOnly() {
      return new BuilderImpl(vertx, connectionProvider, new OptionsOverride() {
        @Override
        public boolean useDatePerAppOnly() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withTransactionControl() {
      return new BuilderImpl(vertx, connectionProvider, new OptionsOverride() {
        @Override
        public boolean allowTransactionControl() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withTransactionControlSilentlyIgnored() {
      return new BuilderImpl(vertx, connectionProvider, new OptionsOverride() {
        @Override
        public boolean ignoreTransactionControl() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public Builder withConnectionAccess() {
      return new BuilderImpl(vertx, connectionProvider, new OptionsOverride() {
        @Override
        public boolean allowConnectionAccess() {
          return true;
        }
      }.withParent(this.options));
    }

    @Override
    public DatabaseProviderVertx create() {
      return new DatabaseProviderVertx(vertx, connectionProvider, options);
    }

    @Override
    public void transact(DbCode tx) {
      create().transact(tx);
    }

    @Override
    public <T> void transactAsync(DbCodeTyped<T> code, Handler<AsyncResult<T>> resultHandler) {
      create().transactAsync(code, resultHandler);
    }

    @Override
    public void transact(DbCodeTx tx) {
      create().transact(tx);
    }

    @Override
    public <T> void transactAsync(DbCodeTypedTx<T> code, Handler<AsyncResult<T>> resultHandler) {
      create().transactAsync(code, resultHandler);
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
      throw new DatabaseException("Called get() on a DatabaseProviderVertx after close()");
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
      public DatabaseProviderVertx create() {
        return new DatabaseProviderVertx(DatabaseProviderVertx.this);
      }

      @Override
      public void transact(DbCode tx) {
        create().transact(tx);
      }

      @Override
      public <T> void transactAsync(DbCodeTyped<T> code, Handler<AsyncResult<T>> resultHandler) {
        create().transactAsync(code, resultHandler);
      }

      @Override
      public void transact(DbCodeTx tx) {
        create().transact(tx);
      }

      @Override
      public <T> void transactAsync(DbCodeTypedTx<T> code, Handler<AsyncResult<T>> resultHandler) {
        create().transactAsync(code, resultHandler);
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
    connectionProvider = null;
  }
}
