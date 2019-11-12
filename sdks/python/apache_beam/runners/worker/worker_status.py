import logging
import sys
import threading
import time
import traceback

import grpc
import queue
from concurrent.futures import ThreadPoolExecutor

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.portability.api.beam_fn_api_pb2 import WorkerStatusRequest
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor


def thread_dump():
  lines = []
  frames = sys._current_frames()  # pylint: disable=protected-access

  for t in threading.enumerate():
    lines.append('--- Thread #%s name: %s ---\n' % (t.ident, t.name))
    lines.append(''.join(traceback.format_stack(frames[t.ident])))

  return ''.join(x.encode('utf-8') for x in lines)


DONE = object()


class FnApiWorkerStatusHandler(object):

  def __init__(self, status_service_descriptor):
    self._alive = True
    ch = GRPCChannelFactory.insecure_channel(status_service_descriptor.url)
    # Make sure the channel is ready to avoid [BEAM-4649]
    grpc.channel_ready_future(ch).result(timeout=60)
    self._status_channel = grpc.intercept_channel(ch, WorkerIdInterceptor())
    self._status_stub = beam_fn_api_pb2_grpc.BeamFnWorkerStatusStub(
      self._status_channel)
    self._responses = queue.Queue()
    self._server = threading.Thread(target=lambda: self._serve(),
                                    name='fn_api_status_handler')
    self._server.daemon = True
    self._server.start()

  def _get_responses(self):
    while True:
      response = self._responses.get()
      if response is DONE:
        self._alive = False
        return
      yield response

  def _serve(self):
    logging.info('FnApiWorkerStatusHandler is now serving')
    while self._alive:
      for request in self._status_stub.WorkerStatus(self._get_responses()):
        logging.info('Received status report request of id %s' %
                     request.request_id)
        self._responses.put(beam_fn_api_pb2.WorkerStatusResponse(
          request_id=request.request_id,
          status_info=thread_dump()))

  def close(self):
    self._responses.put(DONE)


class BeamFnApiWorkerStatusServicer(
  beam_fn_api_pb2_grpc.BeamFnWorkerStatusServicer):

  def __init__(self):
    self.statuses = []

  def WorkerStatus(self, request_iterator, context):
    print('Connected')
    for request in request_iterator:
      print(request.status_info)
      logging.info(request.status_info)
      self.statuses.append(request.status_info)
      time.sleep(1)
      yield WorkerStatusRequest(request_id='1')

if __name__ == '__main__':
    test_worker_status_service = BeamFnApiWorkerStatusServicer()
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    beam_fn_api_pb2_grpc.add_BeamFnWorkerStatusServicer_to_server(
     test_worker_status_service, server)
    test_port = server.add_insecure_port('[::]:0')
    server.start()
    worker_status_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    worker_status_service_descriptor.url = 'localhost:%s' % test_port
    print('server started at %s'% worker_status_service_descriptor)
    fn_status_handler = FnApiWorkerStatusHandler(
      worker_status_service_descriptor)
