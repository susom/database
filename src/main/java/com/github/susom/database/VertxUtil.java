package com.github.susom.database;

import java.util.Map;

import org.slf4j.MDC;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;

/**
 * This is a convenience class to work-around issues with using the SLF4J
 * MDC class. It provides versions of async functionality that preserve
 * the MDC across the event and worker threads.
 *
 * @author garricko
 */
public class VertxUtil {
  /**
   * Wrap a Handler in a way that will preserve the SLF4J MDC context.
   * The context from the current thread at the time of this method call
   * will be cached and restored within the wrapper at the time the
   * handler is invoked.
   */
  public static <T> Handler<T> mdc(Handler<T> handler) {
    Map mdc = MDC.getCopyOfContextMap();

    return t -> {
      Map restore = MDC.getCopyOfContextMap();
      try {
        if (mdc == null) {
          MDC.clear();
        } else {
          MDC.setContextMap(mdc);
        }
        handler.handle(t);
      } finally {
        if (restore == null) {
          MDC.clear();
        } else {
          MDC.setContextMap(restore);
        }
      }
    };
  }

  /**
   * Equivalent to {@link Vertx#executeBlocking(Handler, Handler)},
   * but preserves the {@link MDC} correctly.
   */
  public static <T> void executeBlocking(Vertx vertx, Handler<Future<T>> future, Handler<AsyncResult<T>> handler) {
    executeBlocking(vertx, future, true, handler);
  }

  /**
   * Equivalent to {@link Vertx#executeBlocking(Handler, boolean, Handler)},
   * but preserves the {@link MDC} correctly.
   */
  public static <T> void executeBlocking(Vertx vertx, Handler<Future<T>> future, boolean ordered,
                                         Handler<AsyncResult<T>> handler) {
    vertx.executeBlocking(mdc(future), ordered, mdc(handler));
  }

  /**
   * Equivalent to {@link Vertx#executeBlocking(Handler, Handler)},
   * but preserves the {@link MDC} correctly.
   */
  public static <T> void executeBlocking(WorkerExecutor executor, Handler<Future<T>> future, Handler<AsyncResult<T>> handler) {
    executeBlocking(executor, future, true, handler);
  }

  /**
   * Equivalent to {@link Vertx#executeBlocking(Handler, boolean, Handler)},
   * but preserves the {@link MDC} correctly.
   */
  public static <T> void executeBlocking(WorkerExecutor executor, Handler<Future<T>> future, boolean ordered,
                                         Handler<AsyncResult<T>> handler) {
    executor.executeBlocking(mdc(future), ordered, mdc(handler));
  }
}
