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
 * Use case: One or more transaction ID appears at least twice with the same state.
 */
@RunWith(VertxUnitRunner.class)
public class StatementLoggedTwice {

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
      .setConfig(new JsonObject().put("file.name", "src/test/resources/twiceLogged.log")
      );
    vertx.deployVerticle(LogFileProcessor.class.getName(), options, tc.asyncAssertSuccess());
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
      tc.assertTrue(hasTwiceLineWarning());
      async.complete();
    });
  }

  private boolean hasTwiceLineWarning() {
    String assertOne = "Transaction 'twice' state 'STARTED' has been logged at timestamp 1491377495212 and 1491377495213";
    return stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals(assertOne));
  }

  private boolean hasAnalysedFileRows() {
    return stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals("Total rows analyzed in log file: 5"));
  }

  private boolean hasInsertedToDB() {
    return stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals("Total rows inserted to DB: 1"));
  }

}
