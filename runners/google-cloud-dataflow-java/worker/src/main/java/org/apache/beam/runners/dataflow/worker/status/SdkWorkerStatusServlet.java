/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.status;

import com.google.common.base.Strings;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.runners.dataflow.worker.fn.status.BeamFnSDKStatusGrpcService;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.runners.fnexecution.status.FnApiStatusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SdkWorkerStatusServlet extends BaseStatusServlet implements Capturable {

  private static final Logger LOG = LoggerFactory.getLogger(SdkWorkerStatusServlet.class);
  private final BeamFnSDKStatusGrpcService statusGrpcService;

  public SdkWorkerStatusServlet(BeamFnSDKStatusGrpcService statusGrpcService) {
    super("sdk_status");
    this.statusGrpcService = statusGrpcService;
    LOG.info("SdkWorkerStatusServlet started");
  }

  @Override
  protected String getPath(String parameters) {
    return super.getPath(parameters);
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    String id = request.getParameter("id");
    if (Strings.isNullOrEmpty(id)) {
      // return all connected sdk statuses if no id provided.
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("text/html;charset=utf-8");
      ServletOutputStream writer = response.getOutputStream();
      try (PrintWriter out =
          new PrintWriter(new OutputStreamWriter(writer, StandardCharsets.UTF_8))) {
        captureData(out);
        response.flushBuffer();
        return;
      }
    }

    response.setContentType("text/plain;charset=utf-8");
    Optional<FnApiStatusClient> client = this.statusGrpcService.getStatusClient(id);
    if (!client.isPresent()) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ServletOutputStream writer = response.getOutputStream();
      writer.println(
          String.format("SDK harness with id %s not connected to runner status server.", id));
      response.flushBuffer();
      return;
    }

    CompletableFuture<WorkerStatusResponse> workerStatus = client.get().getWorkerStatus();
    try {
      WorkerStatusResponse workerStatusResponse = workerStatus.get(60, TimeUnit.SECONDS);
      ServletOutputStream writer = response.getOutputStream();
      if (workerStatusResponse.getError().isEmpty()) {
        response.setStatus(HttpServletResponse.SC_OK);
        writer.println(workerStatusResponse.getStatusInfo());
      } else {
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        writer.println(
            String.format("Error from sdk harness: %s", workerStatusResponse.getError()));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Error getting status response from SDK {}", id, e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ServletOutputStream writer = response.getOutputStream();
      writer.println(
          String.format("Error getting sdk status from harness, Exception: %s", e.getMessage()));
    } finally {
      response.flushBuffer();
    }
  }

  @Override
  public String pageName() {
    return "/sdk_status";
  }

  @Override
  public void captureData(PrintWriter writer) {
    writer.println("<html>");
    writer.println("<h1>SDK Harness</h1>");
    // add links to each sdk section for easier navigation.
    for (String sdk : statusGrpcService.getConnectedSdkIds()) {
      writer.print(String.format("<a href=\"#%s\">%s</a> ", sdk, sdk));
    }
    writer.println();

    for (String sdk : statusGrpcService.getConnectedSdkIds()) {
      writer.println(String.format("<h2 id=\"%s\">%s</h2>", sdk, sdk));
      writer.println("<a href=\"#top\">return to top</a>");
      writer.println("<div style=\"white-space:pre-wrap\">");
      try {
        WorkerStatusResponse workerStatusResponse =
            statusGrpcService
                .getStatusClient(sdk)
                .get()
                .getWorkerStatus()
                .get(10, TimeUnit.SECONDS);
        if (workerStatusResponse.getError().isEmpty()) {
          writer.println(workerStatusResponse.getStatusInfo());
        } else {
          writer.println(workerStatusResponse.getError());
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.warn("Error getting status response from SDK {}", sdk, e);
        writer.println(
            String.format(
                "Error getting sdk status from harness %s, Exception: %s", sdk, e.getMessage()));
      }
      writer.println("</div>");
      writer.println("");
    }
    writer.println("</html>");
  }
}
