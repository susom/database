/*
 * Copyright 2019 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.github.susom.database;

import com.github.susom.database.DatabaseProvider.Pool;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * If you are calling this from a Vert.x context, be sure to subscribe using vertx
 * RxHelper.blockingScheduler()
 *
 * <p>This is a lazy provider for Database instances. It helps avoid allocating connection or
 * transaction resources until (or if) we actually need a Database. As a consequence of this
 * laziness, the underlying resources require explicit cleanup by calling either commitAndClose() or
 * rollbackAndClose().
 *
 * @author jmesterh
 * @author garricko
 */
public class DatabaseProviderRx implements Supplier<Database> {
  private static final AtomicInteger poolNameCounter = new AtomicInteger(1);

  private static final Logger log = LoggerFactory.getLogger(DatabaseProviderRx.class);
  private final Options options;
  private DatabaseProviderRx delegateTo = null;
  private Supplier<Connection> connectionProvider;
  private boolean txStarted = false;
  private Connection connection = null;
  private Database database = null;

  public DatabaseProviderRx(Supplier<Connection> connectionProvider, Options options) {
    if (connectionProvider == null) {
      throw new IllegalArgumentException("Connection provider cannot be null");
    }
    this.connectionProvider = connectionProvider;
    this.options = options;
  }

  private DatabaseProviderRx(DatabaseProviderRx delegateTo) {
    this.delegateTo = delegateTo;
    this.options = delegateTo.options;
  }

  /**
   * Configure the database from the following properties read from the provided configuration: <br>
   *
   * <pre>
   *   database.url=...       Database isValid string (required)
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
   * <p>The database flavor will be guessed based on the URL.
   *
   * <p>A database pool will be created using HikariCP.
   *
   * <p>Be sure to retain a copy of the builder so you can call close() later to destroy the pool.
   * You will most likely want to register a JVM shutdown hook to make sure this happens.
   */
  @CheckReturnValue
  public static DatabaseProviderRx.Builder pooledBuilder(Config config) {
    return fromPool(DatabaseProvider.createPool(config));
  }

  /**
   * Use an externally configured DataSource, Flavor, and optionally a shutdown hook. The shutdown
   * hook may be null if you don't want calls to Builder.close() to attempt any shutdown. The
   * DataSource and Flavor are mandatory.
   */
  @CheckReturnValue
  public static DatabaseProviderRx.Builder fromPool(Pool pool) {
    return new DatabaseProviderRx.BuilderImpl(
        () -> {
          if (pool.poolShutdown != null) {
            pool.poolShutdown.close();
          }
        },
        () -> {
          try {
            return pool.dataSource.getConnection();
          } catch (Exception e) {
            throw new DatabaseException("Unable to obtain a connection from DriverManager", e);
          }
        },
        new OptionsDefault(pool.flavor));
  }

  /**
   * Execute a transaction on the current thread, with default semantics (commit if the code
   * completes successfully, or rollback if it throws an error). Note you will get a
   * DatabaseException if you try to call this from the event loop thread. If the provided code
   * block throws anything, it will be wrapped into a DatabaseException unless it was a ThreadDeath
   * or was already a DatabaseException.
   */
  public void transact(final DbCode code) {
    boolean complete = false;
    try {
      code.run(this);
      complete = true;
    } catch (ThreadDeath | DatabaseException t) {
      throw t;
    } catch (Throwable t) {
      throw new DatabaseException("Error during transaction", t);
    } finally {
      if (complete) commitAndClose();
      else rollbackAndClose();
    }
  }

  /**
   * Same as {@link #transact(DbCode)}, but your code block can explicitly manage the behavior of
   * the transaction.
   */
  public void transact(final DbCodeTx code) {
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
      if (complete) commitAndClose();
      else if ((!complete && tx.isRollbackOnError()) || tx.isRollbackOnly()) rollbackAndClose();
      else if (tx.isRollbackOnError()) rollback();
    }
  }

  /**
   * Execute a transaction (no return value) with default semantics (commit if the code completes
   * successfully, or rollback if it throws an error).
   */
  public Completable transactRx(final DbCode code) {
    return Completable.create(
        emitter -> {
          try {
            boolean complete = false;
            try {
              code.run(this);
              complete = true;
            } catch (ThreadDeath | DatabaseException t) {
              throw t;
            } catch (Throwable t) {
              throw new DatabaseException("Error during transaction", t);
            } finally {
              if (complete) commitAndClose();
              else rollbackAndClose();
            }
            emitter.onComplete();
          } catch (Throwable t) {
            emitter.onError(t);
          }
        });
  }

  /**
   * Same as {@link #transactRx(DbCode)}, but your code block can explicitly manage the behavior of
   * the transaction.
   */
  public Completable transactRx(final DbCodeTx code) {
    return Completable.create(
        emitter -> {
          try {
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
            emitter.onComplete();
          } catch (Throwable t) {
            emitter.onError(t);
          }
        });
  }

  /**
   * Execute a transaction, returning the result of the query, with default semantics (commit if the
   * code completes successfully, or rollback if it throws an error). Null return values signal
   * OnComplete(), non-null signal onSuccess()
   */
  public <T> Single<T> transactRx(final DbCodeTyped<T> code) {
    return Single.create(
        emitter -> {
          try {
            T returnValue = null;
            boolean complete = false;
            try {
              returnValue = code.run(this);
              complete = true;
            } catch (ThreadDeath | DatabaseException t) {
              log.error(">>>>>>>>>>>> THREW ERROR: {} {} <<<<<<<<<<<<", t.getMessage(), this.hashCode());
              throw t;
            } catch (Throwable t) {
              log.error(">>>>>>>>>>>> THREW ERROR: {} {} <<<<<<<<<<<<", t.getMessage(), this.hashCode());
              throw new DatabaseException("Error during transaction", t);
            } finally {
              if (complete) commitAndClose();
              else rollbackAndClose();
            }
            emitter.onSuccess(returnValue);
          } catch (Throwable t) {
            emitter.onError(t);
          }
        });
  }

  /** Same as above, but your code block can explicitly manage the behavior of the transaction. */
  public <T> Maybe<T> transactRx(final DbCodeTypedTx<T> code) {
    return Maybe.create(
        emitter -> {
          try {
            T returnValue = null;
            Transaction tx = new TransactionImpl();
            tx.setRollbackOnError(true);
            tx.setRollbackOnly(false);
            boolean complete = false;
            try {
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
            }
            if (returnValue != null) {
              emitter.onSuccess(returnValue);
            } else {
              emitter.onComplete();
            }
          } catch (Throwable t) {
            emitter.onError(t);
          }
        });
  }

  public Database get() {
    if (delegateTo != null) {
      return delegateTo.get();
    }

    if (database != null) {
      return database;
    }

    if (connectionProvider == null) {
      throw new DatabaseException("Called get() on a DatabaseProviderRx after close()");
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
        StringBuilder buf =
            new StringBuilder("Get ").append(options.flavor()).append(" database: ");
        metric.printMessage(buf);
        log.debug(buf.toString());
      }
    }
    return database;
  }

  public DatabaseProviderRx.Builder fakeBuilder() {
    return new DatabaseProviderRx.Builder() {
      @Override
      public DatabaseProviderRx.Builder withOptions(OptionsOverride optionsOverride) {
        return this;
      }

      @Override
      public DatabaseProviderRx.Builder withSqlParameterLogging() {
        return this;
      }

      @Override
      public DatabaseProviderRx.Builder withSqlInExceptionMessages() {
        return this;
      }

      @Override
      public DatabaseProviderRx.Builder withDatePerAppOnly() {
        return this;
      }

      @Override
      public DatabaseProviderRx.Builder withTransactionControl() {
        return this;
      }

      @Override
      public DatabaseProviderRx.Builder withTransactionControlSilentlyIgnored() {
        return this;
      }

      @Override
      public DatabaseProviderRx.Builder withConnectionAccess() {
        return this;
      }

      @Override
      public DatabaseProviderRx create() {
        return new DatabaseProviderRx(DatabaseProviderRx.this);
      }

      @Override
      public void transact(DbCode code) {
        create().transact(code);
      }

      @Override
      public void transact(DbCodeTx code) {
        create().transact(code);
      }

      @Override
      public Completable transactRx(DbCode code) {
        return create().transactRx(code);
      }

      @Override
      public Completable transactRx(DbCodeTx code) {
        return create().transactRx(code);
      }

      @Override
      public <T> Single<T> transactRx(DbCodeTyped<T> code) {
        return create().transactRx(code);
      }

      @Override
      public <T> Maybe<T> transactRx(DbCodeTypedTx<T> code) {
        return create().transactRx(code);
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

  public void rollback() {
    if (delegateTo != null) {
      log.debug("Ignoring rollbackAndClose() because this is a fake provider");
      return;
    }
    log.info("just rolling back, no close and do not set to null");

    if (txStarted) {
      try {
        connection.rollback();
      } catch (Exception e) {
        log.error("Unable to rollback the transaction", e);
      }
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

  /**
   * This builder is immutable, so setting various options does not affect the previous instance.
   * This is intended to make it safe to pass builders around without risk someone will reconfigure
   * it.
   */
  public interface Builder {

    @CheckReturnValue
    DatabaseProviderRx.Builder withOptions(OptionsOverride options);

    /** Enable logging of parameter values along with the SQL. */
    @CheckReturnValue
    DatabaseProviderRx.Builder withSqlParameterLogging();

    /**
     * Include SQL in exception messages. This will also include parameters in the exception
     * messages if SQL parameter logging is enabled. This is handy for development, but be careful
     * as this is an information disclosure risk, dependent on how the exception are caught and
     * handled.
     */
    @CheckReturnValue
    DatabaseProviderRx.Builder withSqlInExceptionMessages();

    /**
     * Wherever argDateNowPerDb() is specified, use argDateNowPerApp() instead. This is useful for
     * testing purposes as you can use OptionsOverride to provide your own system clock that will be
     * used for time travel.
     */
    @CheckReturnValue
    DatabaseProviderRx.Builder withDatePerAppOnly();

    /**
     * Allow provided Database instances to explicitly control transactions using the commitNow()
     * and rollbackNow() methods. Otherwise calling those methods would throw an exception.
     */
    @CheckReturnValue
    DatabaseProviderRx.Builder withTransactionControl();

    /**
     * This can be useful when testing code, as it can pretend to use transactions, while giving you
     * control over whether it actually commits or rolls back.
     */
    @CheckReturnValue
    DatabaseProviderRx.Builder withTransactionControlSilentlyIgnored();

    /**
     * Allow direct access to the underlying database connection. Normally this is not allowed, and
     * is a bad idea, but it can be helpful when migrating from legacy code that works with raw
     * JDBC.
     */
    @CheckReturnValue
    DatabaseProviderRx.Builder withConnectionAccess();

    /**
     * WARNING: You should try to avoid using this method. If you use it more that once or twice in
     * your entire codebase you are probably doing something wrong.
     *
     * <p>If you use this method you are responsible for managing the transaction and
     * commit/rollback/close.
     */
    @CheckReturnValue
    DatabaseProviderRx create();

    void transact(DbCode code);

    void transact(DbCodeTx code);

    Completable transactRx(DbCode code);

    Completable transactRx(DbCodeTx code);

    <T> Single<T> transactRx(DbCodeTyped<T> code);

    <T> Maybe<T> transactRx(DbCodeTypedTx<T> code);

    void close();
  }

  private static class BuilderImpl implements DatabaseProviderRx.Builder {

    private final Supplier<Connection> connectionProvider;
    private final Options options;
    private Closeable pool;

    private BuilderImpl(Closeable pool, Supplier<Connection> connectionProvider, Options options) {
      this.pool = pool;
      this.connectionProvider = connectionProvider;
      this.options = options;
    }

    @Override
    public DatabaseProviderRx.Builder withOptions(OptionsOverride options) {
      return new DatabaseProviderRx.BuilderImpl(
          pool, connectionProvider, options.withParent(this.options));
    }

    @Override
    public DatabaseProviderRx.Builder withSqlParameterLogging() {
      return new DatabaseProviderRx.BuilderImpl(
          pool,
          connectionProvider,
          new OptionsOverride() {
            @Override
            public boolean isLogParameters() {
              return true;
            }
          }.withParent(this.options));
    }

    @Override
    public DatabaseProviderRx.Builder withSqlInExceptionMessages() {
      return new DatabaseProviderRx.BuilderImpl(
          pool,
          connectionProvider,
          new OptionsOverride() {
            @Override
            public boolean isDetailedExceptions() {
              return true;
            }
          }.withParent(this.options));
    }

    @Override
    public DatabaseProviderRx.Builder withDatePerAppOnly() {
      return new DatabaseProviderRx.BuilderImpl(
          pool,
          connectionProvider,
          new OptionsOverride() {
            @Override
            public boolean useDatePerAppOnly() {
              return true;
            }
          }.withParent(this.options));
    }

    @Override
    public DatabaseProviderRx.Builder withTransactionControl() {
      return new DatabaseProviderRx.BuilderImpl(
          pool,
          connectionProvider,
          new OptionsOverride() {
            @Override
            public boolean allowTransactionControl() {
              return true;
            }
          }.withParent(this.options));
    }

    @Override
    public DatabaseProviderRx.Builder withTransactionControlSilentlyIgnored() {
      return new DatabaseProviderRx.BuilderImpl(
          pool,
          connectionProvider,
          new OptionsOverride() {
            @Override
            public boolean ignoreTransactionControl() {
              return true;
            }
          }.withParent(this.options));
    }

    @Override
    public DatabaseProviderRx.Builder withConnectionAccess() {
      return new DatabaseProviderRx.BuilderImpl(
          pool,
          connectionProvider,
          new OptionsOverride() {
            @Override
            public boolean allowConnectionAccess() {
              return true;
            }
          }.withParent(this.options));
    }

    @Override
    public DatabaseProviderRx create() {
      return new DatabaseProviderRx(connectionProvider, options);
    }

    @Override
    public void transact(DbCode code) {
      create().transact(code);
    }

    @Override
    public void transact(DbCodeTx code) {
      create().transact(code);
    }

    @Override
    public Completable transactRx(DbCode code) {
      return create().transactRx(code);
    }

    @Override
    public Completable transactRx(DbCodeTx code) {
      return create().transactRx(code);
    }

    @Override
    public <T> Single<T> transactRx(DbCodeTyped<T> code) {
      return create().transactRx(code);
    }

    @Override
    public <T> Maybe<T> transactRx(DbCodeTypedTx<T> code) {
      return create().transactRx(code);
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
}
