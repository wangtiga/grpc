/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <future>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;


class CallProceeder {
public:
  virtual void OnCallbackGreeterSayHello(void* asyncClientCall) {};
};

  // struct for keeping state and data information
  struct AsyncClientCall {
    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // Storage for the status of the RPC upon completion.
    Status status;

    std::unique_ptr<ClientAsyncResponseReader<HelloReply>> response_reader;

    CallProceeder* call_data_;
  };

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {
  }

  void StartAsyncCompleteRpc() {
    th_async_complete_rpc_.reset(new std::thread(&GreeterClient::AsyncCompleteRpc, this));
  }

  void JoinAsyncCompleteRpc() {
    th_async_complete_rpc_->join();  // blocks forever
  }

  // Assembles the client's payload and sends it to the server.
  void SayHello(const std::string& user, CallProceeder* call_data) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    // Call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;
    call->call_data_ = call_data;

    // stub_->PrepareAsyncSayHello() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->response_reader =
        stub_->PrepareAsyncSayHello(&call->context, request, &cq_);

    // StartCall initiates the RPC call
    call->response_reader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call
    // object.
    call->response_reader->Finish(&call->reply, &call->status, (void*)call);
  }

 private:
  // Loop while listening for completed responses.
  // Prints out the response from the server.
  void AsyncCompleteRpc() {
    void* got_tag;
    bool ok = false;

    // Block until the next result is available in the completion queue "cq".
    while (cq_.Next(&got_tag, &ok)) {
      // The tag in this example is the memory location of the call object
      AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

      // Verify that the request was completed successfully. Note that "ok"
      // corresponds solely to the request for updates introduced by Finish().
      GPR_ASSERT(ok);

      if (call->status.ok()) {
        //std::cout << "AsyncCompleteRpc Greeter received: " << call->reply.message() << std::endl;
      } else {
        std::cout << "AsyncCompleteRpc RPC failed" << std::endl;
      }

      // Once we're complete, deallocate the call object.
      //delete call;
      call->call_data_->OnCallbackGreeterSayHello(call);
    }
  }

  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Greeter::Stub> stub_;

  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq_;

  // thread AsyncCompleteRpc
  std::unique_ptr<std::thread> th_async_complete_rpc_;
};

  // Class encompasing the state and logic needed to serve a request.
  class CallData : public CallProceeder {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    CallData(Greeter::AsyncService* service, ServerCompletionQueue* cq, GreeterClient* client_rpc)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), client_rpc_(client_rpc) {
    //CallData(Greeter::AsyncService* service, ServerCompletionQueue* cq)
    //    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      // Invoke the serving logic right away.
      Proceed();
    }

    void Proceed();
    void OnCallbackGreeterSayHello(void* asyncClientCall) override;

   private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    Greeter::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue* cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    // What we get from the client.
    HelloRequest request_;
    // What we send back to the client.
    HelloReply reply_;
    HelloReply sub_reply1_;
    HelloReply sub_reply2_;

    // The means to get back to the client.
    ServerAsyncResponseWriter<HelloReply> responder_;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS1, PROCESS2, PROCESS3, PROCESS4, PROCESS_LAST, FINISH };
    CallStatus status_;  // The current serving state.

    GreeterClient* client_rpc_;
  };


class ServerImpl {
 public:
  // There is no shutdown handling in this code.
  void RunServer();
  void StopServer();

 private:
  void HandleRpcsMultiThread(int num_threads);
  void HandleRpcs(ServerCompletionQueue* cq, GreeterClient* client_rpc);

  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;

  //std::unique_ptr<ServerCompletionQueue> cq_;
  //std::unique_ptr<GreeterClient> client_rpc_;

  std::vector<std::unique_ptr<ServerCompletionQueue>> cq_array_;
  std::vector<std::unique_ptr<GreeterClient>> client_rpc_array_;
};

int main(int argc, char** argv) {
  ServerImpl server;
  server.RunServer();
  server.StopServer();
  return 0;
}

  // There is no shutdown handling in this code.
  void ServerImpl::RunServer() {

    //client_rpc_.reset(new GreeterClient(grpc::CreateChannel(
    //  "localhost:50052", grpc::InsecureChannelCredentials())));
    //client_rpc_->StartAsyncCompleteRpc();

    std::string server_address("0.0.0.0:50051");
    std::string external_rpc_address("192.168.1.3:50051");

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    //cq_ = builder.AddCompletionQueue();

    int num_threads = std::thread::hardware_concurrency();
    num_threads = 4;
    std::cout << "hardware_concurrency thread num " << num_threads << std::endl;
    for (int i = 0; i < num_threads; ++i) {
      std::unique_ptr<ServerCompletionQueue> cq = builder.AddCompletionQueue();
      std::unique_ptr<GreeterClient> client_rpc(new GreeterClient(
        grpc::CreateChannel("external_rpc_address:50051", grpc::InsecureChannelCredentials())
      ));
      client_rpc->StartAsyncCompleteRpc();
      cq_array_.push_back(std::move(cq));
      client_rpc_array_.push_back(std::move(client_rpc));
    }

    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    //HandleRpcs();
    HandleRpcsMultiThread(num_threads);
  }

  void ServerImpl::HandleRpcsMultiThread(int num_threads) {
    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; ++i) {
      auto cq = cq_array_[i].get();
      auto client_rpc = client_rpc_array_[i].get();
      futures.push_back(std::async(std::launch::async, [this, cq, client_rpc]() {
        // Proceed to the server's main loop.
        //HandleRpcs(cq_.get(), client_rpc_.get());
        HandleRpcs(cq, client_rpc);
      }));
    }

    for (auto&& f : futures) {
      f.wait();
    }
  }

  void ServerImpl::StopServer() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    //cq_->Shutdown();
    for (int i = 0; i < cq_array_.size(); ++i) {
      cq_array_[i]->Shutdown();
    }
    for (int i = 0; i < client_rpc_array_.size(); ++i) {
      client_rpc_array_[i]->JoinAsyncCompleteRpc();
    }
  }

  // This can be run in multiple threads if needed.
  void ServerImpl::HandleRpcs(ServerCompletionQueue* cq, GreeterClient* client_rpc) {
    // Spawn a new CallData instance to serve new clients.
    new CallData(&service_, cq, client_rpc);
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(cq->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallData*>(tag)->Proceed();
    }
  }


    void CallData::OnCallbackGreeterSayHello(void* asyncClientCall) {
      AsyncClientCall* call = static_cast<AsyncClientCall*>(asyncClientCall);
      if (status_ == PROCESS2) {
        sub_reply1_ = call->reply;
      } else if (status_ == PROCESS_LAST) {
        sub_reply2_ = call->reply;
      } else {
        GPR_ASSERT(false);
      }

      delete call;

      Proceed();
    }

    void CallData::Proceed() {
      if (status_ == CREATE) {
        // Make this instance progress to the PROCESS state.
        status_ = PROCESS1;
        status_ = PROCESS_LAST; // TODO don not send one request

        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_,
                                  this);
      } else if (status_ == PROCESS1) {
        status_ = PROCESS2;
        //status_ = PROCESS_LAST; // TODO only send one request
        client_rpc_->SayHello("rpc1", this);
      } else if (status_ == PROCESS2) {
        status_ = PROCESS_LAST;
        client_rpc_->SayHello("rpc2", this);
      } else if (status_ == PROCESS_LAST) {

        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new CallData(service_, cq_, client_rpc_);

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = FINISH;

        // The actual processing.
        std::string prefix("Hello ");
        prefix = prefix + " " + request_.name();
        prefix = prefix + " sub_reply1: " + sub_reply1_.message();
        prefix = prefix + " sub_reply2: " + sub_reply2_.message();
        reply_.set_message(prefix);
        responder_.Finish(reply_, Status::OK, this);
      } else {
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }
