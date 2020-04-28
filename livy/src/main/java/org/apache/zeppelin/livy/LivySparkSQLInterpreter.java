/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.livy;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.InterpreterUtils;
import org.apache.zeppelin.interpreter.ResultMessages;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Livy SparkSQL Interpreter for Zeppelin.
 */
public class LivySparkSQLInterpreter extends BaseLivyInterpreter {
  public static final String ZEPPELIN_LIVY_SPARK_SQL_FIELD_TRUNCATE =
      "zeppelin.livy.spark.sql.field.truncate";

  public static final String ZEPPELIN_LIVY_SPARK_SQL_MAX_RESULT =
      "zeppelin.livy.spark.sql.maxResult";

  private LivySparkInterpreter sparkInterpreter;
  private String codeType = null;

  private boolean isSpark2 = false;
  private int maxResult = 1000;
  private boolean truncate = true;

  public LivySparkSQLInterpreter(Properties property) {
    super(property);
    this.maxResult = Integer.parseInt(property.getProperty(ZEPPELIN_LIVY_SPARK_SQL_MAX_RESULT));
    if (property.getProperty(ZEPPELIN_LIVY_SPARK_SQL_FIELD_TRUNCATE) != null) {
      this.truncate =
          Boolean.parseBoolean(property.getProperty(ZEPPELIN_LIVY_SPARK_SQL_FIELD_TRUNCATE));
    }
  }

  @Override
  public String getSessionKind() {
    return "spark";
  }

  @Override
  public void open() throws InterpreterException {
    this.sparkInterpreter = getInterpreterInTheSameSessionByClassName(LivySparkInterpreter.class);
    // As we don't know whether livyserver use spark2 or spark1, so we will detect SparkSession
    // to judge whether it is using spark2.
    try {
      InterpreterContext context = InterpreterContext.builder()
          .setInterpreterOut(new InterpreterOutput(null))
          .build();
      InterpreterResult result = sparkInterpreter.interpret("spark", context);
      if (result.code() == InterpreterResult.Code.SUCCESS &&
          result.message().get(0).getData().contains("org.apache.spark.sql.SparkSession")) {
        LOGGER.info("SparkSession is detected so we are using spark 2.x for session {}",
            sparkInterpreter.getSessionInfo().id);
        isSpark2 = true;
      } else {
        // spark 1.x
        result = sparkInterpreter.interpret("sqlContext", context);
        if (result.code() == InterpreterResult.Code.SUCCESS) {
          LOGGER.info("sqlContext is detected.");
        } else if (result.code() == InterpreterResult.Code.ERROR) {
          // create SqlContext if it is not available, as in livy 0.2 sqlContext
          // is not available.
          LOGGER.info("sqlContext is not detected, try to create SQLContext by ourselves");
          result = sparkInterpreter.interpret(
              "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n"
                  + "import sqlContext.implicits._", context);
          if (result.code() == InterpreterResult.Code.ERROR) {
            throw new LivyException("Fail to create SQLContext," +
                result.message().get(0).getData());
          }
        }
      }
    } catch (LivyException e) {
      throw new RuntimeException("Fail to Detect SparkVersion", e);
    }
  }

  @Override
  public InterpreterResult interpret(String line, InterpreterContext context) {
    try {
      if (StringUtils.isEmpty(line)) {
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, "");
      }

      // use triple quote so that we don't need to do string escape.
      String sqlQuery = null;
      if (isSpark2) {
        sqlQuery = "spark.sql(\"\"\"" + line + "\"\"\").show(" + maxResult + ", " +
            truncate + ")";
      } else {
        sqlQuery = "sqlContext.sql(\"\"\"" + line + "\"\"\").show(" + maxResult + ", " +
            truncate + ")";
      }
      InterpreterResult result = sparkInterpreter.interpret(sqlQuery, context);
      if (result.code() == InterpreterResult.Code.SUCCESS) {
        InterpreterResult result2 = new InterpreterResult(InterpreterResult.Code.SUCCESS);
        for (InterpreterResultMessage message : result.message()) {
          // convert Text type to Table type. We assume the text type must be the sql output. This
          // assumption is correct for now. Ideally livy should return table type. We may do it in
          // the future release of livy.
          if (message.getType() == InterpreterResult.Type.TEXT) {
            List<String> rows = parseSQLOutput(message.getData());
            result2.add(InterpreterResult.Type.TABLE, StringUtils.join(rows, "\n"));
            if (rows.size() >= (maxResult + 1)) {
              result2.add(ResultMessages.getExceedsLimitRowsMessage(maxResult,
                  ZEPPELIN_LIVY_SPARK_SQL_MAX_RESULT));
            }
          } else {
            result2.add(message.getType(), message.getData());
          }
        }
        return result2;
      } else {
        return result;
      }
    } catch (Exception e) {
      LOGGER.error("Exception in LivySparkSQLInterpreter while interpret ", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR,
          InterpreterUtils.getMostRelevantMessage(e));
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  protected List<String> parseSQLOutput(String output) {
    List<String> rows = new ArrayList<>();
    for (String line : output.split("\n")) {
      // Skip lines of the form '+---+---+'
      if (line.matches("^\\+(?:-*\\+)*$")) {
        continue;
      }
      List<String> cells = new ArrayList<>();
      String[] entries = StringUtils.split(line, "\\|");
      for (String entry : entries) {
        cells.add(
          StringEscapeUtils.unescapeJava(StringEscapeUtils.escapeEcmaScript(entry)).trim()
        );
      }
      rows.add(StringUtils.join(cells, "\t"));
    }
    return rows;
  }

  public boolean concurrentSQL() {
    return Boolean.parseBoolean(getProperty("zeppelin.livy.concurrentSQL"));
  }

  @Override
  public Scheduler getScheduler() {
    if (concurrentSQL()) {
      int maxConcurrency = 10;
      return SchedulerFactory.singleton().createOrGetParallelScheduler(
          LivySparkInterpreter.class.getName() + this.hashCode(), maxConcurrency);
    } else {
      if (sparkInterpreter != null) {
        return sparkInterpreter.getScheduler();
      } else {
        return null;
      }
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    if (this.sparkInterpreter != null) {
      sparkInterpreter.cancel(context);
    }
  }

  @Override
  public void close() {
    if (this.sparkInterpreter != null) {
      this.sparkInterpreter.close();
    }
  }

  @Override
  public int getProgress(InterpreterContext context) {
    if (this.sparkInterpreter != null) {
      return this.sparkInterpreter.getProgress(context);
    } else {
      return 0;
    }
  }

  @Override
  protected String extractAppId() throws LivyException {
    // it wont' be called because it would delegate to LivySparkInterpreter
    throw new UnsupportedOperationException();
  }

  @Override
  protected String extractWebUIAddress() throws LivyException {
    // it wont' be called because it would delegate to LivySparkInterpreter
    throw new UnsupportedOperationException();
  }
}
