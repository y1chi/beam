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
package org.apache.beam.runners.dataflow.worker.fn.status;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc.BeamFnWorkerStatusImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.runners.fnexecution.status.FnApiStatusClient;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamFnSDKStatusGrpcService extends BeamFnWorkerStatusImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnSDKStatusGrpcService.class);
  private final HeaderAccessor headerAccessor;
  private final Map<String, FnApiStatusClient> connectedClient = new ConcurrentHashMap<>();

  private BeamFnSDKStatusGrpcService(
      ApiServiceDescriptor apiServiceDescriptor, HeaderAccessor headerAccessor) {
    this.headerAccessor = headerAccessor;
    LOG.info("Launched Beam Fn Status service at {}", apiServiceDescriptor);
  }

  public static BeamFnSDKStatusGrpcService create(
      ApiServiceDescriptor apiServiceDescriptor, HeaderAccessor headerAccessor) {
    if (apiServiceDescriptor == null || Strings.isNullOrEmpty(apiServiceDescriptor.getUrl())) {
      return null;
    }
    return new BeamFnSDKStatusGrpcService(apiServiceDescriptor, headerAccessor);
  }

  @Override
  public void close() throws Exception {}

  @Override
  public StreamObserver<WorkerStatusResponse> workerStatus(
      StreamObserver<WorkerStatusRequest> requestObserver) {
    final String workerId = headerAccessor.getSdkWorkerId();
    if (Strings.isNullOrEmpty(workerId)) {
      // SDK harness status report should be inforced with proper sdk worker id.
      // If SDK harness connect without proper sdk worker id, return noop stream observer to ignore
      // the status report from this SDK as there is no way to identify it.
      LOG.error("No worker_id header provided in status response.");
      return new StreamObserver<WorkerStatusResponse>() {
        @Override
        public void onNext(WorkerStatusResponse workerStatusResponse) {}

        @Override
        public void onError(Throwable throwable) {}

        @Override
        public void onCompleted() {}
      };
    }

    LOG.info("Beam Fn Status client connected with id {}", workerId);
    FnApiStatusClient fnApiStatusClient =
        FnApiStatusClient.forRequestObserver(workerId, requestObserver);
    connectedClient.put(workerId, fnApiStatusClient);
    return fnApiStatusClient.getResponseObserver();
  }

  public Optional<FnApiStatusClient> getStatusClient(String workerId) {
    return Optional.ofNullable(this.connectedClient.getOrDefault(workerId, null));
  }

  public Set<String> getConnectedSdkIds() {
    TreeSet<String> result =
        new TreeSet<>(
            (first, second) -> {
              if (first.length() != second.length()) {
                return first.length() - second.length();
              }
              return first.compareTo(second);
            });
    result.addAll(this.connectedClient.keySet());
    return result;
  }
}
