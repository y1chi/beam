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
package org.apache.beam.runners.fnexecution.status;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.sdk.fn.stream.SynchronizedStreamObserver;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FnApiStatusClient implements Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(FnApiStatusClient.class);
  private final StreamObserver<WorkerStatusRequest> requestReceiver;
  private final ResponseStreamObserver responseObserver = new ResponseStreamObserver();
  private final Map<String, CompletableFuture<WorkerStatusResponse>> responseQueue =
      new ConcurrentHashMap<>();
  private final String workerId;
  private AtomicBoolean isClosed = new AtomicBoolean(false);

  private FnApiStatusClient(String workerId, StreamObserver<WorkerStatusRequest> requestReceiver) {
    this.requestReceiver = SynchronizedStreamObserver.wrapping(requestReceiver);
    this.workerId = workerId;
  }

  public static FnApiStatusClient forRequestObserver(
      String workerId, StreamObserver<WorkerStatusRequest> requestObserver) {
    return new FnApiStatusClient(workerId, requestObserver);
  }

  public CompletableFuture<WorkerStatusResponse> getWorkerStatus() {
    WorkerStatusRequest request =
        WorkerStatusRequest.newBuilder().setRequestId(UUID.randomUUID().toString()).build();
    return getWorkerStatus(request);
  }

  public CompletableFuture<WorkerStatusResponse> getWorkerStatus(WorkerStatusRequest request) {
    CompletableFuture<WorkerStatusResponse> future = new CompletableFuture<>();
    this.requestReceiver.onNext(request);
    this.responseQueue.put(request.getRequestId(), future);
    return future;
  }

  @Override
  public void close() throws IOException {}

  public String getWorkerId() {
    return this.workerId;
  }

  public StreamObserver<WorkerStatusResponse> getResponseObserver() {
    return responseObserver;
  }

  private class ResponseStreamObserver implements StreamObserver<WorkerStatusResponse> {

    @Override
    public void onNext(WorkerStatusResponse response) {
      if (!responseQueue.isEmpty()) {
        CompletableFuture<WorkerStatusResponse> responseFuture =
            responseQueue.remove(response.getRequestId());
        if (response.getError().isEmpty()) {
          responseFuture.complete(response);
        } else {
          responseFuture.completeExceptionally(
              new RuntimeException(
                  String.format(
                      "Error received from SDK harness for getting worker status %s",
                      response.getError())));
        }
      }
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("{} received error {}", FnApiStatusClient.class.getSimpleName(), throwable);
    }

    @Override
    public void onCompleted() {}
  }
}
