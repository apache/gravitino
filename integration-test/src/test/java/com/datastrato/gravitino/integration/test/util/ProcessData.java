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
package com.datastrato.gravitino.integration.test.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// zeppelin-integration/src/test/java/org/apache/zeppelin/ProcessData.java
public class ProcessData {
  public enum TypesOfData {
    OUTPUT,
    ERROR,
    EXIT_CODE,
    STREAMS_MERGED,
    PROCESS_DATA_OBJECT
  }

  public static final Logger LOG = LoggerFactory.getLogger(ProcessData.class);

  private Process checkedProcess;
  private boolean printToConsole = false;

  public ProcessData(
      Process connected_process, boolean printToConsole, int silenceTimeout, TimeUnit timeUnit) {
    this.checkedProcess = connected_process;
    this.printToConsole = printToConsole;
    this.silenceTimeout = TimeUnit.MILLISECONDS.convert(silenceTimeout, timeUnit);
  }

  public ProcessData(Process connected_process, boolean printToConsole, int silenceTimeoutSec) {
    this.checkedProcess = connected_process;
    this.printToConsole = printToConsole;
    this.silenceTimeout = TimeUnit.MILLISECONDS.convert(silenceTimeoutSec, TimeUnit.SECONDS);
  }

  public ProcessData(Process connected_process, boolean printToConsole) {
    this.checkedProcess = connected_process;
    this.printToConsole = printToConsole;
  }

  public ProcessData(Process connectedProcess) {
    this.checkedProcess = connectedProcess;
    this.printToConsole = true;
  }

  boolean returnCodeRetrieved = false;

  private String outPutStream = null;
  private String errorStream = null;
  private int returnCode;
  private long silenceTimeout = 10 * 60 * 1000;
  private final long unconditionalExitDelayMinutes = 30;

  public static boolean isRunning(Process process) {
    try {
      process.exitValue();
      return false;
    } catch (IllegalThreadStateException e) {
      return true;
    }
  }

  public Object getData(TypesOfData type) {
    // TODO get rid of Pseudo-terminal will not be allocated because stdin is not a terminal.
    switch (type) {
      case OUTPUT:
        {
          return this.getOutPutStream(false);
        }
      case ERROR:
        {
          return this.getErrorStream();
        }
      case EXIT_CODE:
        {
          return this.getExitCodeValue();
        }
      case STREAMS_MERGED:
        {
          return this.getOutPutStream(true);
        }
      case PROCESS_DATA_OBJECT:
        {
          this.getErrorStream();
          return this;
        }
      default:
        {
          throw new IllegalArgumentException("Data Type " + type + " not supported yet!");
        }
    }
  }

  public int getExitCodeValue() {
    try {
      if (!returnCodeRetrieved) {
        this.checkedProcess.waitFor();
        this.returnCode = this.checkedProcess.exitValue();
        this.returnCodeRetrieved = true;
        this.checkedProcess.destroy();
      }
    } catch (Exception inter) {
      throw new RuntimeException(
          "Couldn't finish waiting for process " + this.checkedProcess + " termination", inter);
    }
    return this.returnCode;
  }

  public String getOutPutStream(boolean mergeStreams) {
    if (this.outPutStream == null) {
      try {
        buildOutputAndErrorStreamData(mergeStreams);
      } catch (Exception e) {
        throw new RuntimeException(
            "Couldn't retrieve Output Stream data from process: " + this.checkedProcess.toString(),
            e);
      }
    }
    this.outPutStream =
        this.outPutStream.replace(
            "Pseudo-terminal will not be allocated because stdin is not a terminal.", "");
    this.errorStream =
        this.errorStream.replace(
            "Pseudo-terminal will not be allocated because stdin is not a terminal.", "");
    return this.outPutStream;
  }

  public String getErrorStream() {
    if (this.errorStream == null) {
      try {
        buildOutputAndErrorStreamData(false);
      } catch (Exception e) {
        throw new RuntimeException(
            "Couldn't retrieve Error Stream data from process: " + this.checkedProcess.toString(),
            e);
      }
    }
    this.outPutStream =
        this.outPutStream.replace(
            "Pseudo-terminal will not be allocated because stdin is not a terminal.", "");
    this.errorStream =
        this.errorStream.replace(
            "Pseudo-terminal will not be allocated because stdin is not a terminal.", "");
    return this.errorStream;
  }

  public String toString() {
    return String.format("[OUTPUT STREAM]\n%s\n", this.outPutStream)
        + String.format("[ERROR STREAM]\n%s\n", this.errorStream)
        + String.format("[EXIT CODE]\n%d", this.returnCode);
  }

  private void buildOutputAndErrorStreamData(boolean mergeStreams) throws IOException {
    StringBuilder sbInStream = new StringBuilder();
    StringBuilder sbErrorStream = new StringBuilder();

    try {
      InputStream in = this.checkedProcess.getInputStream();
      InputStream inErrors = this.checkedProcess.getErrorStream();
      BufferedReader inReader =
          new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      BufferedReader inReaderErrors =
          new BufferedReader(new InputStreamReader(inErrors, StandardCharsets.UTF_8));
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Started retrieving data from streams of attached process: {}", this.checkedProcess);
      }
      // Store start time to be able to finish method if command hangs
      long lastStreamDataTime = System.currentTimeMillis();

      // Stop after 'unconditionalExitDelayMinutes' even if process is alive and sending output
      long unconditionalExitTime =
          System.currentTimeMillis()
              + TimeUnit.MILLISECONDS.convert(unconditionalExitDelayMinutes, TimeUnit.MINUTES);
      final int BUFFER_LEN = 300;
      char[] charBuffer =
          new char[BUFFER_LEN]; // Use char buffer to read output, size can be tuned.
      boolean outputProduced = true; // Flag to check if previous iteration produced any output

      // Continue if process is alive or some output was produced on previous iteration
      // and there may be still some data to read.
      while (isRunning(this.checkedProcess) || outputProduced) {
        outputProduced = false;

        // Some local commands can exit fast, but immediate stream reading will give no output and
        // after iteration, 'while' condition will be false so we will not read out any output while
        // it is still there, just need to wait for some time for it to appear in streams.
        GravitinoITUtils.sleep(100, false);

        StringBuilder tempSB = new StringBuilder();
        while (inReader.ready()) {
          tempSB.setLength(0); // clean temporary StringBuilder
          int readCount =
              inReader.read(charBuffer, 0, BUFFER_LEN); // read up to 'BUFFER_LEN' chars to buffer
          if (readCount < 1) { // if nothing read or error occurred
            break;
          }
          tempSB.append(charBuffer, 0, readCount);

          sbInStream.append(tempSB);
          if (tempSB.length() > 0) {
            // set flag to know that we read something and there may be moire data, even if process
            // already exited
            outputProduced = true;
          }

          // remember last time data was read from streams to be sure we are not looping infinitely
          lastStreamDataTime = System.currentTimeMillis();
        }

        tempSB = new StringBuilder(); // Same, but for error stream
        while (inReaderErrors.ready()) {
          tempSB.setLength(0);
          int readCount = inReaderErrors.read(charBuffer, 0, BUFFER_LEN);
          if (readCount < 1) {
            break;
          }
          tempSB.append(charBuffer, 0, readCount);
          if (mergeStreams) {
            sbInStream.append(tempSB);
          } else {
            sbErrorStream.append(tempSB);
          }
          if (tempSB.length() > 0) {
            outputProduced = true;
            String temp = new String(tempSB);
            temp =
                temp.replaceAll(
                    "Pseudo-terminal will not be allocated because stdin is not a terminal.", "");
            // TODO : error stream output need to be improved, because it outputs downloading
            //  information.
            if (printToConsole) {
              if (!temp.trim().equals("")) {
                if (temp.toLowerCase().contains("error") || temp.toLowerCase().contains("failed")) {
                  LOG.warn(temp.trim());
                } else {
                  LOG.debug(temp.trim());
                }
              }
            }
          }
          lastStreamDataTime = System.currentTimeMillis();
        }

        // Exit if silenceTimeout ms has passed from last stream read. Means process is alive but
        // not sending any data.
        if ((System.currentTimeMillis() - lastStreamDataTime > silenceTimeout)
            ||
            // Exit unconditionally - guards against alive process continuously sending data.
            (System.currentTimeMillis() > unconditionalExitTime)) {

          LOG.info(
              "Conditions: "
                  + (System.currentTimeMillis() - lastStreamDataTime > silenceTimeout)
                  + " "
                  + (System.currentTimeMillis() > unconditionalExitTime));
          this.checkedProcess.destroy();
          try {
            if ((System.currentTimeMillis() > unconditionalExitTime)) {
              LOG.error(
                  "!Unconditional exit occured!\nsome process hag up for more than "
                      + unconditionalExitDelayMinutes
                      + " minutes.");
            }
            LOG.error("!##################################!");
            StringWriter sw = new StringWriter();
            Exception e = new Exception("Exited from buildOutputAndErrorStreamData by timeout");
            e.printStackTrace(new PrintWriter(sw)); // Get stack trace
            LOG.error(String.valueOf(e), e);
          } catch (Exception ignore) {
            LOG.info("Exception in ProcessData while buildOutputAndErrorStreamData ", ignore);
          }
          break;
        }
      }

      in.close();
      inErrors.close();
    } finally {
      this.outPutStream = sbInStream.toString();
      this.errorStream = sbErrorStream.toString();
    }
  }
}
