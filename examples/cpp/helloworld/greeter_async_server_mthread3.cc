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
  //virtual void OnCallbackGreeterSayHello(void* asyncClientCall) {};
  virtual void Proceed() {};
};

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {
  }
  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Greeter::Stub> stub_;
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
    void ProceedSendGreeterSayHello(const std::string& user);
    void ProceedSendGreeterSayHello2();

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

    // The means to get back to the client.
    ServerAsyncResponseWriter<HelloReply> responder_;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS1, PROCESS2, PROCESS3, PROCESS4, PROCESS_LAST, FINISH };
    CallStatus status_;  // The current serving state.

    GreeterClient* client_rpc_;
    ClientContext client_rpc_context1_;
    ClientContext client_rpc_context2_;
    std::unique_ptr<ClientAsyncResponseReader<HelloReply>> sub_response_reader1_;
    std::unique_ptr<ClientAsyncResponseReader<HelloReply>> sub_response_reader2_;
    HelloReply sub_reply1_;
    HelloReply sub_reply2_;
    Status sub_status1_;
    Status sub_status2_;
  };


class ServerImpl {
 public:
  // There is no shutdown handling in this code.
  void RunServer(int num_threads, int max_calldata_in_queue);
  void StopServer();

 private:
  void HandleRpcsMultiThread(int num_threads, int max_calldata_in_queue);
  void HandleRpcs(ServerCompletionQueue* cq, GreeterClient* client_rpc, int max_calldata_in_queue);

  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;

  //std::unique_ptr<ServerCompletionQueue> cq_;
  //std::unique_ptr<GreeterClient> client_rpc_;

  std::vector<std::unique_ptr<ServerCompletionQueue>> cq_array_;
  std::vector<std::unique_ptr<GreeterClient>> client_rpc_array_;
};

int main(int argc, char** argv) {
  int num_threads = std::thread::hardware_concurrency();
  int max_calldata_in_queue = 1000; 
  if (argc >= 2) {
    num_threads = atoi(argv[1]);
  }
  if (argc >= 3) {
    max_calldata_in_queue = atoi(argv[2]);
  }

  ServerImpl server;
  server.RunServer(num_threads, max_calldata_in_queue);
  server.StopServer();
  return 0;
}

  // There is no shutdown handling in this code.
  void ServerImpl::RunServer(int num_threads, int max_calldata_in_queue) {

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

    std::cout << "hardware_concurrency thread num " << num_threads << std::endl;
    for (int i = 0; i < num_threads; ++i) {
      std::unique_ptr<ServerCompletionQueue> cq = builder.AddCompletionQueue();
      std::unique_ptr<GreeterClient> client_rpc(new GreeterClient(
        grpc::CreateChannel("external_rpc_address:50051", grpc::InsecureChannelCredentials())
      ));
      cq_array_.push_back(std::move(cq));
      client_rpc_array_.push_back(std::move(client_rpc));
    }

    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    //HandleRpcs();
    HandleRpcsMultiThread(num_threads, max_calldata_in_queue);
  }

  void ServerImpl::HandleRpcsMultiThread(int num_threads, int max_calldata_in_queue) {
    //std::vector<std::future<void>> futures;
    //for (int i = 0; i < num_threads; ++i) {
    //  auto cq = cq_array_[i].get();
    //  auto client_rpc = client_rpc_array_[i].get();
    //  futures.push_back(std::async(std::launch::async, [this, cq, client_rpc]() {
    //    // Proceed to the server's main loop.
    //    //HandleRpcs(cq_.get(), client_rpc_.get());
    //    HandleRpcs(cq, client_rpc);
    //  }));
    //}
    //for (auto&& f : futures) {
    //  f.wait();
    //}

    std::vector<std::unique_ptr<std::thread>> workers;
    for (int i = 0; i < num_threads; ++i) {
      auto cq = cq_array_[i].get();
      auto client_rpc = client_rpc_array_[i].get();
      std::unique_ptr<std::thread> worker(new std::thread([this, cq, client_rpc, max_calldata_in_queue] {
        HandleRpcs(cq, client_rpc, max_calldata_in_queue);
      }));
      workers.push_back(std::move(worker));
    }

    for (auto& f : workers) {
      f->join();
    }
  }

  void ServerImpl::StopServer() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    //cq_->Shutdown();
    for (int i = 0; i < cq_array_.size(); ++i) {
      cq_array_[i]->Shutdown();
    }
  }

  // This can be run in multiple threads if needed.
  void ServerImpl::HandleRpcs(ServerCompletionQueue* cq, GreeterClient* client_rpc, int max_calldata_in_queue) {
    // Spawn a new CallData instance to serve new clients.
    std::cout << "HandleRpcs max_calldata_in_queue: " << max_calldata_in_queue << std::endl;
    for (int i = 0; i < max_calldata_in_queue; ++i) {
      new CallData(&service_, cq, client_rpc);
    }
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

    void CallData::ProceedSendGreeterSayHello(const std::string& user) {
      HelloRequest request;
      request.set_name(user);

      // stub_->PrepareAsyncSayHello() creates an RPC object, returning
      // an instance to store in "call" but does not actually start the RPC
      // Because we are using the asynchronous API, we need to hold on to
      // the "call" instance in order to get updates on the ongoing RPC.
      sub_response_reader1_ = 
          client_rpc_->stub_->PrepareAsyncSayHello(&client_rpc_context1_, request, cq_);

      // StartCall initiates the RPC call
      sub_response_reader1_->StartCall();

      // Request that, upon completion of the RPC, "reply" be updated with the
      // server's response; "status" with the indication of whether the operation
      // was successful. Tag the request with the memory address of the call
      // object.
      sub_response_reader1_->Finish(&sub_reply1_, &sub_status1_, (void*)this);
    }

    void CallData::ProceedSendGreeterSayHello2() {
      if (sub_status1_.ok()) {
        //std::cout << "ProceedSendGreeterSayHello2 Greeter received: " << sub_reply1_.message() << std::endl;
      } else {
        std::cout << "ProceedSendGreeterSayHello2 RPC failed" << std::endl;
      }
    }

    void CallData::Proceed() {
      if (status_ == CREATE) {
        // Make this instance progress to the PROCESS state.
        status_ = PROCESS1;
        //status_ = PROCESS_LAST; // TODO don not send one request

        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_,
                                  this);
      } else if (status_ == PROCESS1) {
        status_ = PROCESS2;
        status_ = PROCESS_LAST; // TODO only send one request
        //client_rpc_->SayHello("rpc1", this);
        ProceedSendGreeterSayHello("OneQueue_rpc1");
      } else if (status_ == PROCESS2) {
        status_ = PROCESS_LAST;
        //client_rpc_->SayHello("rpc2", this);
        //ProceedSendGreeterSayHello("OneQueue_rpc2");
      } else if (status_ == PROCESS_LAST) {
        ProceedSendGreeterSayHello2();

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
