package com.interview.cs.loganalyser;


import com.google.gson.Gson;
import io.vertx.core.Future;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.parsetools.RecordParser;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;

import java.util.concurrent.ConcurrentHashMap;

public class MainVerticle extends AbstractVerticle {


  private long threshold = 4;
  private JsonObject config = new JsonObject()
    .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
    .put("driver_class", "org.hsqldb.jdbcDriver")
    .put("max_pool_size", 30);
  Gson gson = new Gson();
  private static final String INSERT_RAW_LOG = "insert into test values(?,?,?,?,?)";
  //first iteration, duration=timestamp and alert=state
  private static final String CREATE_TABLE = "create table test(id varchar(10), duration bigint, type varchar(50), host varchar(20), alert varchar(10))";
  private static final String SELECT_BY_LOG_ID = "select * from test where id = ?";

  private JDBCClient client;


  @Override
  public void start(Future<Void> fut) {

    long started = System.currentTimeMillis();

    AsyncFile asyncFile = vertx.fileSystem().openBlocking("sampleHUGE.log", new OpenOptions().setRead(true).setWrite(false).setCreate(false));


    client = JDBCClient.createShared(vertx, config);

    client.getConnection(conn -> {
      ConcurrentHashMap<String, LogLine> tempMap = new ConcurrentHashMap<>();
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
              client.getConnection(conn2 -> {
                //  System.out.println("Read data: " + data);
                LogLine logLine = gson.fromJson(data.toJsonObject().toString(), LogLine.class);
                //  System.out.println("LogLine id: "+ logLine.id);
                LogLine logInMap = tempMap.get(logLine.id);
                if (null == logInMap) {
                  tempMap.put(logLine.id, logLine);
                } else {
                  //TODO handle error case when both state are the same (wrong log statement in the code)
                  //TODO handle error case when transaction id are not unique
                  long delta = Math.abs(logInMap.timestamp - logLine.timestamp);
                  //System.out.println("id: " + logLine.id + " - delta: " + delta);
                  String alert = "false";
                  if (delta >= threshold) {
                    alert = "true";
                  }

                  JsonArray params = new JsonArray().add(logLine.id).add(delta).add(logLine.type == null ? "" : logLine.type).add(logLine.host == null ? "" : logLine.host).add(alert);


                  conn2.result().queryWithParams(INSERT_RAW_LOG, params, insertResult -> {
                    if (insertResult.failed()) {
                      System.err.println("Error inserting " + data.toString() + ": " + insertResult.cause());
                      return;
                    }
                    //System.out.println("Removing log id from temp map: " + logInMap.id);
                    tempMap.remove(logInMap.id);
                    conn2.result().close();

                  });

                }


              });


            });//end record parser


            asyncFile.handler(recordParser).endHandler(v -> {
              asyncFile.close();
              System.out.println("Done inserting to DB");
              System.out.println("TempMap size /1: " + tempMap.size());

              connection.close(done -> {
                System.out.println("DB Connection closed.");
                System.out.println("TempMap size /2: " + tempMap.size());
                System.out.println("Completed in :" + (System.currentTimeMillis() - started));
                if (done.failed()) {
                  throw new RuntimeException(done.cause());
                }
              });


              /*connection.queryStream("select * from test", selectall -> {
                selectall.result().handler(row -> {
                  // System.out.println(row.encode());
                }).endHandler(endOfSelect -> connection.close(done -> {
                  System.out.println("DB Connection closed.");
                  System.out.println("TempMap size /2: " + tempMap.size());
                  System.out.println("Completed in :" + (System.currentTimeMillis() - started));
                  if (done.failed()) {
                    throw new RuntimeException(done.cause());
                  }
                }));*/
              //});
            });//end async file handler
          }
        });
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


