package com.github.susom.database.test;

import java.io.File;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.github.susom.database.Config;
import com.github.susom.database.DatabaseProviderVertx;
import com.github.susom.database.DatabaseProviderVertx.Builder;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Make sure the SLF4J MDC feature behaves well with Vertx.
 */
@RunWith(VertxUnitRunner.class)
public class VertxLoggingTest {
  private Logger log = LoggerFactory.getLogger(VertxLoggingTest.class);
  int times = 0;

  static {
    // We will put all Derby related files inside ./build to keep our working copy clean
    File directory = new File("target").getAbsoluteFile();
    if (directory.exists() || directory.mkdirs()) {
      System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
    }
  }

  @Test
  public void testMdcTransferToWorkerDatabase(TestContext context) {
    Async async = context.async();

    Vertx vertx = Vertx.vertx();

    Config config = Config.from().value("database.url", "jdbc:derby:target/testdb;create=true").get();
    Builder db = DatabaseProviderVertx.pooledBuilder(vertx, config).withSqlParameterLogging();

    vertx.createHttpServer()
        .requestHandler(r -> {
          context.assertTrue(MDC.getCopyOfContextMap() == null || MDC.getCopyOfContextMap().isEmpty(),
              "Was: " + MDC.getCopyOfContextMap());
          MDC.put("userId", "joe");
          MDC.put("requestId", "1");
          log.debug("Request before blocking: " + MDC.getCopyOfContextMap());
          db.transactAsync(dbs -> {
            log.debug("Request inside blocking: " + MDC.getCopyOfContextMap());
            context.assertEquals("joe", MDC.get("userId"));
            context.assertEquals("1", MDC.get("requestId"));
            return null;
          }, result -> {
            log.debug("Request after blocking: " + MDC.getCopyOfContextMap());
            context.assertEquals("joe", MDC.get("userId"));
            context.assertEquals("1", MDC.get("requestId"));

            db.transactAsync(dbs -> {
              log.debug("Nested request inside blocking: " + MDC.getCopyOfContextMap());
              context.assertEquals("joe", MDC.get("userId"));
              context.assertEquals("1", MDC.get("requestId"));
              return null;
            }, result2 -> {
              log.debug("Nested request after blocking: " + MDC.getCopyOfContextMap());
              context.assertEquals("joe", MDC.get("userId"));
              context.assertEquals("1", MDC.get("requestId"));
              db.transactAsync(dbs -> {
                log.debug("Nested2 request inside blocking: " + MDC.getCopyOfContextMap());
                context.assertEquals("joe", MDC.get("userId"));
                context.assertEquals("1", MDC.get("requestId"));
                return null;
              }, result3 -> {
                log.debug("Nested2 request after blocking: " + MDC.getCopyOfContextMap());
                context.assertEquals("joe", MDC.get("userId"));
                context.assertEquals("1", MDC.get("requestId"));
                r.response().end();
                MDC.clear();
              });
              MDC.put("userId", "bob");
            });
            MDC.put("userId", "bob");
          });

          // Simulate the event loop doing something else while we
          // are processing, which may mess up the MDC
          MDC.put("userId", "bob");
        }).listen(8101, server -> {
      vertx.createHttpClient().get(8101, "localhost", "/foo").handler(response ->
          context.assertEquals(200, response.statusCode())
      ).end();
    });

    vertx.setPeriodic(100, id -> {
      // Repeat enough times to cycle through all workers
      if (times++ > 100) {
        async.complete();
      }
      context.assertTrue(MDC.getCopyOfContextMap() == null || MDC.getCopyOfContextMap().isEmpty(),
          "Was: " + MDC.getCopyOfContextMap());
      MDC.put("userId", "<timer>");
      log.debug("Timer before blocking: " + MDC.getCopyOfContextMap());
      db.transactAsync(dbs -> {
        log.debug("Timer inside blocking: " + MDC.getCopyOfContextMap());
        if (!"<timer>".equals(MDC.get("userId"))) {
          System.out.println("No match");
        }
        context.assertEquals("<timer>", MDC.get("userId"));
        context.assertNull(MDC.get("requestId"));
        return null;
      }, result -> {
        log.debug("Timer after blocking: " + MDC.getCopyOfContextMap());
        context.assertEquals("<timer>", MDC.get("userId"));
        context.assertNull(MDC.get("requestId"));
        db.transactAsync(dbs -> {
          log.debug("Timer nested inside blocking: " + MDC.getCopyOfContextMap());
          if (!"<timer>".equals(MDC.get("userId"))) {
            System.out.println("No match");
          }
          context.assertEquals("<timer>", MDC.get("userId"));
          context.assertNull(MDC.get("requestId"));
          return null;
        }, result2 -> {
          log.debug("Timer nested after blocking: " + MDC.getCopyOfContextMap());
          context.assertEquals("<timer>", MDC.get("userId"));
          context.assertNull(MDC.get("requestId"));
          db.transactAsync(dbs -> {
            log.debug("Timer nested2 inside blocking: " + MDC.getCopyOfContextMap());
            if (!"<timer>".equals(MDC.get("userId"))) {
              System.out.println("No match");
            }
            context.assertEquals("<timer>", MDC.get("userId"));
            context.assertNull(MDC.get("requestId"));
            return null;
          }, result3 -> {
            log.debug("Timer nested2 after blocking: " + MDC.getCopyOfContextMap());
            context.assertEquals("<timer>", MDC.get("userId"));
            context.assertNull(MDC.get("requestId"));
            MDC.clear();
          });
          MDC.put("userId", "jim");
        });
        MDC.put("userId", "jim");
      });

      // Simulate the event loop doing something else while we
      // are processing, which may mess up the MDC
      MDC.put("userId", "jim");
    });
  }
}
