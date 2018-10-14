package com.interview.cs.loganalyser;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

/**
 * Use case: One or more of the log statements either miss ID, STATE, TIMESTAMP or all of those.
 */
@RunWith(VertxUnitRunner.class)
public class MalformedLogStatement {

  private Vertx vertx;
  private List<LoggingEvent> stdoutLogEvents;

  @Before
  public void setUp(TestContext tc) {
    stdoutLogEvents = new ArrayList<>();
    vertx = Vertx.vertx();
    LogManager.getRootLogger().getAppender("stdout").addFilter(new Filter() {
      @Override
      public int decide(LoggingEvent event) {
        stdoutLogEvents.add(event);
        return 0;
      }
    });
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("file.name", "src/test/resources/malformedTransactionLog.log")
      );
    vertx.deployVerticle(MainVerticle.class.getName(), options, tc.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext tc) {
    vertx.close(tc.asyncAssertSuccess());
  }

  @Test
  public void verify_log_statements(TestContext tc) {
    vertx.setTimer(1000, t -> {
      Async async = tc.async();
      tc.assertTrue(hasInsertedToDB());
      tc.assertTrue(hasAnalysedFileRows());
      tc.assertTrue(hasMalformedWarning());
      async.complete();
    });
  }

  private boolean hasMalformedWarning() {
    String assertOne = "Malformed transaction log in 'src/test/resources/malformedTransactionLog.log' at line 1";
    String assertTwo = "Malformed transaction log in 'src/test/resources/malformedTransactionLog.log' at line 2";
    String assertThree = "Malformed transaction log in 'src/test/resources/malformedTransactionLog.log' at line 4";
    return stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals(assertOne))
      && stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals(assertOne))
      && stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals(assertThree));
  }

  private boolean hasAnalysedFileRows() {
    return stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals("Total rows analyzed in log file: 5"));
  }

  private boolean hasInsertedToDB() {
    return stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals("Total rows inserted to DB: 1"));
  }

}
