package com.interview.cs.loganalyser;


import com.google.gson.Gson;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.parsetools.RecordParser;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.rxjava.core.parsetools.RecordParser.newDelimited;

public class MainVerticle extends AbstractVerticle {

  private static final Logger LOG = Logger.getLogger(MainVerticle.class);

  private static final long threshold = 4;// in ms, if |FINISHED-STARTED| >= threshold then alert

  private JsonObject dbConfig = new JsonObject()
    .put("url", "jdbc:hsqldb:file:db/TESTDB?shutdown=true")
    .put("driver_class", "org.hsqldb.jdbcDriver")
    .put("max_pool_size", 50) // tried to find the value that gives better performance
    .put("auto_commit_on_close", true);

  private static final Gson GSON = new Gson(); //Json parser

  private static final String TABLE_NAME = "LOG_MONITOR";
  private static final String INSERT_RAW_LOG = "INSERT INTO " + TABLE_NAME + " VALUES(?,?,?,?,?)";
  private static final String CREATE_TABLE = "CREATE TEXT TABLE IF NOT EXISTS " + TABLE_NAME + "( id varchar(10), duration bigint, type varchar(50), host varchar(20), alert varchar(10))";
  private static final String SET_TABLE = "SET TABLE " + TABLE_NAME + " SOURCE \"logmonitor;ignore_first=false;all_quoted=true;cache_rows=10000;cache_size=1000\"";

  private final AtomicInteger insertedRecords = new AtomicInteger();
  private final AtomicInteger analyzedLogLines = new AtomicInteger();

  private JDBCClient jdbcClient;
  private String fileName;
  private static final HashMap<String, String> tempMap = new HashMap<>();//used to temporarily store log statements read from file

  public MainVerticle() {
    InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE); // this is needed to fix a log quirk in netty
  }


  @Override
  public void start(Future<Void> fut) throws Exception {
    WelcomeMessage.howTo();
    retrieveInputFileName();
    cleanUpDBFiles();
    long started = System.currentTimeMillis();
    jdbcClient = JDBCClient.createShared(vertx, dbConfig);
    jdbcClient.getConnection(conn -> {
      if (conn.failed()) {
        LOG.error("FATAL: could not obtain db connection: " + conn.cause().getMessage(), conn.cause());
        fut.fail(conn.cause());
        throw new RuntimeException(conn.cause());
      }
      final SQLConnection connection = conn.result();
      connection.execute(CREATE_TABLE, res -> {
        if (res.failed()) {
          connection.close();
          fut.fail(res.cause());
          throw new RuntimeException(res.cause());
        } else {
          connection.execute(SET_TABLE, settable -> { //SET is needed to be able to persist the DB file
            processInputFile(fut, started, connection);
          });// end SET table
        }// else
      });// end create table
    });// end get connection
  }

  private void processInputFile(Future<Void> fut, long started, SQLConnection connection) {
    vertx.fileSystem().open(fileName, new OpenOptions().setRead(true).setWrite(false).setCreate(false), inputFileIsOpen -> {

      if (inputFileIsOpen.failed()) {
        LOG.fatal("Could not open input file: " + fileName, inputFileIsOpen.cause());
        fut.fail(inputFileIsOpen.cause());
        throw new RuntimeException(inputFileIsOpen.cause());
      }

      AsyncFile asyncFile = inputFileIsOpen.result();

      asyncFile
        .handler(logLineProcessor())
        .endHandler(v -> {
          asyncFile.close();
          closeDown(started, tempMap, connection);
          fut.complete();
          vertx.close();
        });///end async file handler
    });
  }

  private void cleanUpDBFiles() {
    if (vertx.fileSystem().existsBlocking("db/")) {
      vertx.fileSystem().deleteRecursiveBlocking("db/", true);
      LOG.info("Data from the previous run has been deleted");
    }
  }

  private void retrieveInputFileName() {
    fileName = config().getString("file.name");
    LOG.info("File to be analysed: " + fileName);
  }

  private void logLineProcessor(Map<String, String> tempMap, Buffer rawLogLine, Handler<Buffer> bufferHandler) {

    if (analyzedLogLines.incrementAndGet() % 10000 == 0) {
      System.out.printf("\rSo far analyzed: " + analyzedLogLines.intValue() + " inserted: " + insertedRecords.intValue());
    }
    LogLine logLine = GSON.fromJson(rawLogLine.toJsonObject().toString(), LogLine.class);
    if (StringUtil.isNullOrEmpty(logLine.id) || StringUtil.isNullOrEmpty(logLine.state) || logLine.timestamp == 0) {
      LOG.warn("Malformed transaction log in '" + fileName + "' at line " + analyzedLogLines.intValue());
      return;
    }
    LogLine logInMap = GSON.fromJson(tempMap.get(logLine.id), LogLine.class);
    if (null == logInMap) {
      tempMap.put(logLine.id, rawLogLine.toJsonObject().toString());
    } else {
      if (logInMap.state.equals(logLine.state)) {
        LOG.warn("Transaction '" + logInMap.id + "' state '" + logInMap.state + "' has been logged at timestamp " + logInMap.timestamp + " and " + logLine.timestamp);
        return;
      }
      tempMap.remove(logLine.id);
      //TODO handle error case when both state are the same (wrong log statement in the code)
      long delta = Math.abs(logInMap.timestamp - logLine.timestamp);
      String alert = "false";
      if (delta >= threshold) {
        alert = "true";
      }
      JsonArray params = new JsonArray()
        .add(logLine.id)
        .add(delta)
        .add(logLine.type == null ? "" : logLine.type)
        .add(logLine.host == null ? "" : logLine.host)
        .add(alert);
      bufferHandler.handle(new Buffer(params.toBuffer()));
    }
  }

  private void closeDown(long started, Map<String, String> tempMap, SQLConnection connection) {
    LOG.info("Done inserting to DB");
    LOG.debug("TempMap size: " + tempMap.size());
    if (tempMap.size() != 0) {
      LOG.warn("The following log transaction miss either STARTED or FINISHED:");
      tempMap.values().stream().forEach(r -> LOG.warn(r));
    }

    connection.close(done -> {
      LOG.debug("DB Connection closed.");
      LOG.info("Completed in :" + (System.currentTimeMillis() - started));
      LOG.info("Total rows inserted to DB: " + insertedRecords.getAndIncrement());
      LOG.info("Total rows analyzed in log file: " + analyzedLogLines.getAndIncrement());
      if (done.failed()) {
        LOG.error("Exception while closing the DB connection during the closeDown procedure", done.cause());
        throw new RuntimeException(done.cause());
      }

    });
  }

  private void singleSaveBuffer(Buffer jsonArray, Future complete) {

    jdbcClient.getConnection(connection -> {
      SQLConnection singleConnection = connection.result();
      singleConnection.queryWithParams(INSERT_RAW_LOG, jsonArray.toJsonArray(), insertResult -> {
        if (insertResult.failed()) {
          LOG.warn("WARNING: Error inserting " + jsonArray.toJsonArray() + ": " + insertResult.cause().getMessage(), insertResult.cause());
          return;
        }
        LOG.debug("Inserted: " + jsonArray.toJsonArray());
        singleConnection.close();
        complete.complete();
      });
    });
  }

  /**
   * DTO for the lines of the file to be analysed
   */
  private class LogLine {
    String id;
    String state;
    String type;
    String host;
    long timestamp;
  }

  /**
   * Describe the chain of events from the log being read from input till saved to DB once both state are found
   *
   * @return a RecordParser with the logic to process a log statement and update the DB
   */
  private RecordParser logLineProcessor() {
    return newDelimited("\n",
      processedLogLine -> logLineProcessor(tempMap, processedLogLine,
        insertParameters -> singleSaveBuffer(insertParameters,
          Future.future(r -> insertedRecords.incrementAndGet()))));
  }

}


