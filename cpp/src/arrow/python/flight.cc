// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <signal.h>
#include <utility>

#include "arrow/flight/internal.h"
#include "arrow/python/flight.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace py {
namespace flight {

PyServerAuthHandler::PyServerAuthHandler(PyObject* handler,
                                         PyServerAuthHandlerVtable vtable)
    : vtable_(vtable) {
  Py_INCREF(handler);
  handler_.reset(handler);
}

Status PyServerAuthHandler::Authenticate(arrow::flight::ServerAuthSender* outgoing,
                                         arrow::flight::ServerAuthReader* incoming) {
  PyAcquireGIL lock;
  vtable_.authenticate(handler_.obj(), outgoing, incoming);
  return CheckPyError();
}

Status PyServerAuthHandler::IsValid(const std::string& token,
                                    std::string* peer_identity) {
  PyAcquireGIL lock;
  vtable_.is_valid(handler_.obj(), token, peer_identity);
  return CheckPyError();
}

PyClientAuthHandler::PyClientAuthHandler(PyObject* handler,
                                         PyClientAuthHandlerVtable vtable)
    : vtable_(vtable) {
  Py_INCREF(handler);
  handler_.reset(handler);
}

Status PyClientAuthHandler::Authenticate(arrow::flight::ClientAuthSender* outgoing,
                                         arrow::flight::ClientAuthReader* incoming) {
  PyAcquireGIL lock;
  vtable_.authenticate(handler_.obj(), outgoing, incoming);
  return CheckPyError();
}

Status PyClientAuthHandler::GetToken(std::string* token) {
  PyAcquireGIL lock;
  vtable_.get_token(handler_.obj(), token);
  return CheckPyError();
}

PyFlightServer::PyFlightServer(PyObject* server, PyFlightServerVtable vtable)
    : vtable_(vtable) {
  Py_INCREF(server);
  server_.reset(server);
}

Status PyFlightServer::ListFlights(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Criteria* criteria,
    std::unique_ptr<arrow::flight::FlightListing>* listings) {
  PyAcquireGIL lock;
  vtable_.list_flights(server_.obj(), context, criteria, listings);
  return CheckPyError();
}

Status PyFlightServer::GetFlightInfo(const arrow::flight::ServerCallContext& context,
                                     const arrow::flight::FlightDescriptor& request,
                                     std::unique_ptr<arrow::flight::FlightInfo>* info) {
  PyAcquireGIL lock;
  vtable_.get_flight_info(server_.obj(), context, request, info);
  return CheckPyError();
}

Status PyFlightServer::DoGet(const arrow::flight::ServerCallContext& context,
                             const arrow::flight::Ticket& request,
                             std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
  PyAcquireGIL lock;
  vtable_.do_get(server_.obj(), context, request, stream);
  return CheckPyError();
}

Status PyFlightServer::DoPut(const arrow::flight::ServerCallContext& context,
                             std::unique_ptr<arrow::flight::FlightMessageReader> reader) {
  PyAcquireGIL lock;
  vtable_.do_put(server_.obj(), context, std::move(reader));
  return CheckPyError();
}

Status PyFlightServer::DoAction(const arrow::flight::ServerCallContext& context,
                                const arrow::flight::Action& action,
                                std::unique_ptr<arrow::flight::ResultStream>* result) {
  PyAcquireGIL lock;
  vtable_.do_action(server_.obj(), context, action, result);
  return CheckPyError();
}

Status PyFlightServer::ListActions(const arrow::flight::ServerCallContext& context,
                                   std::vector<arrow::flight::ActionType>* actions) {
  PyAcquireGIL lock;
  vtable_.list_actions(server_.obj(), context, actions);
  return CheckPyError();
}

Status PyFlightServer::ServeWithSignals() {
  // Respect the current Python settings, i.e. only interrupt the server if there is
  // an active signal handler for SIGINT and SIGTERM.
  std::vector<int> signals;
  for (const int signum : {SIGINT, SIGTERM}) {
    struct sigaction handler;
    int ret = sigaction(signum, nullptr, &handler);
    if (ret != 0) {
      return Status::IOError("sigaction call failed");
    }
    if (handler.sa_handler != SIG_DFL && handler.sa_handler != SIG_IGN) {
      signals.push_back(signum);
    }
  }
  RETURN_NOT_OK(SetShutdownOnSignals(signals));

  // Serve until we got told to shutdown or a signal interrupted us
  RETURN_NOT_OK(Serve());
  int signum = GotSignal();
  if (signum != 0) {
    // Issue the signal again with Python's signal handlers restored
    PyAcquireGIL lock;
    raise(signum);
    // XXX Ideally we would loop and serve again if no exception was raised.
    // Unfortunately, gRPC will return immediately if Serve() is called again.
    ARROW_UNUSED(PyErr_CheckSignals());
  }

  return Status::OK();
}

PyFlightResultStream::PyFlightResultStream(PyObject* generator,
                                           PyFlightResultStreamCallback callback)
    : callback_(callback) {
  Py_INCREF(generator);
  generator_.reset(generator);
}

Status PyFlightResultStream::Next(std::unique_ptr<arrow::flight::Result>* result) {
  PyAcquireGIL lock;
  callback_(generator_.obj(), result);
  return CheckPyError();
}

PyFlightDataStream::PyFlightDataStream(
    PyObject* data_source, std::unique_ptr<arrow::flight::FlightDataStream> stream)
    : stream_(std::move(stream)) {
  Py_INCREF(data_source);
  data_source_.reset(data_source);
}

std::shared_ptr<arrow::Schema> PyFlightDataStream::schema() { return stream_->schema(); }

Status PyFlightDataStream::Next(arrow::flight::FlightPayload* payload) {
  return stream_->Next(payload);
}

PyGeneratorFlightDataStream::PyGeneratorFlightDataStream(
    PyObject* generator, std::shared_ptr<arrow::Schema> schema,
    PyGeneratorFlightDataStreamCallback callback)
    : schema_(schema), callback_(callback) {
  Py_INCREF(generator);
  generator_.reset(generator);
}

std::shared_ptr<arrow::Schema> PyGeneratorFlightDataStream::schema() { return schema_; }

Status PyGeneratorFlightDataStream::Next(arrow::flight::FlightPayload* payload) {
  PyAcquireGIL lock;
  callback_(generator_.obj(), payload);
  return CheckPyError();
}

Status CreateFlightInfo(const std::shared_ptr<arrow::Schema>& schema,
                        const arrow::flight::FlightDescriptor& descriptor,
                        const std::vector<arrow::flight::FlightEndpoint>& endpoints,
                        uint64_t total_records, uint64_t total_bytes,
                        std::unique_ptr<arrow::flight::FlightInfo>* out) {
  arrow::flight::FlightInfo::Data flight_data;
  RETURN_NOT_OK(arrow::flight::internal::SchemaToString(*schema, &flight_data.schema));
  flight_data.descriptor = descriptor;
  flight_data.endpoints = endpoints;
  flight_data.total_records = total_records;
  flight_data.total_bytes = total_bytes;
  arrow::flight::FlightInfo value(flight_data);
  *out = std::unique_ptr<arrow::flight::FlightInfo>(new arrow::flight::FlightInfo(value));
  return Status::OK();
}

}  // namespace flight
}  // namespace py
}  // namespace arrow
