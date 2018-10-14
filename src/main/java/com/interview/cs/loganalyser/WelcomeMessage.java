package com.interview.cs.loganalyser;

import org.apache.log4j.Logger;

public class WelcomeMessage {

  private static final Logger LOG = Logger.getLogger(WelcomeMessage.class);

  public static void howTo() {
    LOG.info("************* HOW TO USE THE LOG ANALYSER *******************");
    LOG.info("The file to be analysed has to be specified in a json config file.");
    LOG.info("The json config file content is in the format:");
    LOG.info("{");
    LOG.info("\t\"file.name\":\"<absolute file path>\"");
    LOG.info("}");
    LOG.info("Invoke command: 'java -jar loganalyzer-vertx-1.0-SNAPSHOT-fat.jar -conf <path to conf file>");
    LOG.info("************************************************************\n");
  }

}
