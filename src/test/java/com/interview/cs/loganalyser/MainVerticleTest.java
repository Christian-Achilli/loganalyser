package com.interview.cs.loganalyser;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {

  private Vertx vertx;

  @Before
  public void setUp(TestContext tc) {
    vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("file.name", "src/main/resources/smallsample.log")
      );
    vertx.deployVerticle(MainVerticle.class.getName(), options, tc.asyncAssertSuccess(s -> System.out.println("verticle deployed")));
  }

  @After
  public void tearDown(TestContext tc) {
    vertx.close(tc.asyncAssertSuccess());
  }

  AtomicInteger rowCounter = new AtomicInteger();

  @Test
  public void db_was_created(TestContext tc) {
    Async async = tc.async();
    AsyncFile dbFile = vertx.fileSystem().openBlocking("db/logmonitor", new OpenOptions().setRead(true).setWrite(false).setCreate(false));

    RecordParser assertParser = RecordParser.newDelimited("\n", line -> rowCounter.incrementAndGet());

    dbFile.handler(assertParser).endHandler(end -> {
      tc.assertEquals(3, rowCounter.get());
      async.complete();
    });

  /*  vertx.createHttpClient().getNow(8080, "localhost", "/", response -> {
      tc.assertEquals(response.statusCode(), 200);
      response.bodyHandler(body -> {
        tc.assertTrue(body.length() > 0);
        async.complete();
      });
    });*/
  }

}
