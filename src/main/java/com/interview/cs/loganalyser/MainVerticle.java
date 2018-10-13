package com.interview.cs.loganalyser;


import com.google.gson.Gson;
import io.vertx.core.Future;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.parsetools.RecordParser;
import io.vertx.rxjava.core.shareddata.LocalMap;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {


  private long threshold = 4;
  private JsonObject config = new JsonObject()
    .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
    .put("driver_class", "org.hsqldb.jdbcDriver")
    .put("max_pool_size", 50)
    .put("auto_commit_on_close", true);
  Gson gson = new Gson();
  private static final String INSERT_RAW_LOG = "insert into test values(?,?,?,?,?)";
  //first iteration, duration=timestamp and alert=state
  private static final String CREATE_TABLE = "create table test( id varchar(10), duration bigint, type varchar(50), host varchar(20), alert varchar(10))";
  private static final String SELECT_BY_LOG_ID = "select * from test where id = ?";

  private JDBCClient client;


  @Override
  public void start(Future<Void> fut) {

    AtomicInteger counter = new AtomicInteger();

    long started = System.currentTimeMillis();

    AsyncFile asyncFile = vertx.fileSystem().openBlocking("smallsample.log", new OpenOptions().setRead(true).setWrite(false).setCreate(false));

    client = JDBCClient.createShared(vertx, config);

    client.getConnection(conn -> {
      LocalMap<String, String> tempMap = vertx.sharedData().getLocalMap("log-ids");
      List<JsonArray> parametersList = new ArrayList<>();

      //System.out.println("TempMap size: "+tempMap.size());
      if (conn.failed()) {
        System.err.println(conn.cause().getMessage());
        return;
      }

      final SQLConnection connection = conn.result();

      //drop
      connection.execute("drop table test", dropres -> {
        if (dropres.failed()) {
          System.err.println(dropres.cause());
        }
        //create
        connection.execute(CREATE_TABLE, res -> {
          if (res.failed()) {
            connection.close();
            throw new RuntimeException(res.cause());
          } else {

            RecordParser recordParser = RecordParser.newDelimited("\n", data -> {

              LogLine logLine = gson.fromJson(data.toJsonObject().toString(), LogLine.class);
              System.out.println("LogLine id: " + logLine.id);
              LogLine logInMap = gson.fromJson(tempMap.get(logLine.id), LogLine.class);
              if (null == logInMap) {
                tempMap.put(logLine.id, data.toJsonObject().toString());
              } else {
                //TODO handle error case when both state are the same (wrong log statement in the code)
                //TODO handle error case when transaction id are not unique
                long delta = Math.abs(logInMap.timestamp - logLine.timestamp);
                //System.out.println("id: " + logLine.id + " - delta: " + delta);
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


                parametersList.add(params);

                if (counter.incrementAndGet() % 10000 == 0) {
                  batchSave(counter, tempMap, parametersList, null);
                } //end if counter


              }


            });//end record parser


            asyncFile.handler(recordParser).endHandler(v -> {
              asyncFile.close();

              Future endBatchSave = Future.future(n -> closeDown(counter, started, tempMap, connection));
              if (parametersList.size() != 0) {
                System.out.println("saving the last batch");
                batchSave(counter, tempMap, parametersList, endBatchSave);
              } else {
                closeDown(counter, started, tempMap, connection);
              }

            });//end async file handler
          }
        });
      });
    });
  }

  private void closeDown(AtomicInteger counter, long started, LocalMap<String, String> tempMap, SQLConnection connection) {
    System.out.println("Done inserting to DB");
    System.out.println("TempMap size /1: " + tempMap.size());
    AtomicInteger selectCounter = new AtomicInteger();
    connection.queryStream("select * from test", selectall -> {
      selectall.result().handler(row -> {
        System.out.println(row.encode());
        selectCounter.getAndIncrement();
      }).endHandler(endOfSelect -> connection.close(done -> {
        System.out.println("DB Connection closed.");
        System.out.println("TempMap size /2: " + tempMap.size());
        System.out.println("Completed in :" + (System.currentTimeMillis() - started));
        System.out.println("Total rows to DB: " + selectCounter.get());
        System.out.println("Total rows analyzed: " + counter.get());
        if (done.failed()) {
          throw new RuntimeException(done.cause());
        }
      }));
    });
  }

  private void batchSave(AtomicInteger counter, LocalMap<String, String> tempMap, List<JsonArray> parametersList, Future endBatchSave) {

    List<JsonArray> copyList = parametersList.stream().map(JsonArray::copy).collect(Collectors.toList());
    System.out.println("Number of elements to batchsave: " + copyList.size());
    System.out.println("Inserting to DB: " + counter.intValue() + " - parameters list size: " + copyList.size());
    client.getConnection(conn2 -> {
      SQLConnection batchConnection = conn2.result();
      batchConnection.batchWithParams(INSERT_RAW_LOG, copyList, insertResult -> {
        if (insertResult.failed()) {
          System.err.println("Error inserting " + parametersList.size() + ": " + insertResult.cause());
          insertResult.cause().printStackTrace();
          return;
        }
        copyList.stream().forEach(jsonPar -> tempMap.remove(jsonPar.getString(0)));
        parametersList.clear();
        batchConnection.close();
        System.out.println("Inserting to DB complete: " + counter.intValue() + " - parameters list size: " + copyList.size());
        if (endBatchSave != null) {
          endBatchSave.complete();
        }
      });

    });
  }

  class LogLine {
    String id;
    String state;
    String type;
    String host;
    long timestamp;

  }

}


