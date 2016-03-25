package com.github.susom.database;

import java.util.Map;

import org.slf4j.MDC;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * This is a convenience class to work-around issues with using the SLF4J
 * MDC class. It provides versions of async functionality that preserve
 * the MDC across the event and worker threads.
 *
 * @author garricko
 */
public class VertxUtil {
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
    Map mdc = MDC.getCopyOfContextMap();

    vertx.<T>executeBlocking(f -> {
      try {
        if (mdc != null) {
          MDC.setContextMap(mdc);
        }
        future.handle(f);
      } finally {
        MDC.clear();
      }
    }, ordered, h -> {
      try {
        if (mdc != null) {
          MDC.setContextMap(mdc);
        }
        handler.handle(h);
      } finally {
        MDC.clear();
      }
    });
  }
}
