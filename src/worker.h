#pragma once

#include <iostream>
#include <unordered_set>
#include <map>
#include <fstream>
#include <sstream>
#include <grpc++/grpc++.h>
#include <mr_task_factory.h>
#include <string>

#include "mr_tasks.h"
#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using masterworker::WorkerRequest;
using masterworker::WorkerReply;
using masterworker::ShardComp;
using masterworker::MapReduceWorker;

using namespace std;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

enum JobType {MAP=0, REDUCE=1};

// Reference for much of the grpc boilerplate: https://grpc.io/docs/languages/cpp/async/

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {
public:
	/* DON'T change the function signature of this constructor */
	Worker(std::string ip_addr_port);

	/* DON'T change this function's signature */
	bool run();

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	/* -- Member functions -- */
	// RPC handler from gRPC documentation.
	void Handler();
	// Functions for getting modifying map_fn's internal members from CallData.
	static void setNOutputFiles (shared_ptr<BaseMapper>& map_fn, int input) {
		map_fn->impl_->n_output_files_ = input;
	}
	static void setTmpLoc(shared_ptr<BaseMapper>& map_fn, string input) {
		map_fn->impl_->tmp_loc_ = input;
	}
	static void setNOutputFilesReducer (shared_ptr<BaseReducer>& reduce_fn, int input) {
		reduce_fn->impl_->n_output_files_ = input;
	}
	static void setOutputLoc(shared_ptr<BaseReducer>& reduce_fn, string input) {
		reduce_fn->impl_->output_loc_ = input;
	}
	static unordered_set<string> getIntermediateFiles(shared_ptr<BaseMapper>& map_fn) {
		return map_fn->impl_->intermediate_files_;
	}

	/* -- Data Members -- */
	// Keep track of worker ip address and port (for debugging purposes)
	string worker_addr_;
	// CallData class from gRPC documentation
	friend class CallData;
	// Server
	unique_ptr<Server> server_;
	// Completion queue
	unique_ptr<ServerCompletionQueue> cq_;
	// MapReduceWorker's AsyncService
	MapReduceWorker::AsyncService service_;
	// Get the intermediate files.
	unordered_set<string> final_intermediate_files_;
};


// Reference: https://github.com/grpc/grpc/blob/v1.33.2/examples/cpp/helloworld/greeter_async_server.cc
class CallData {
public:
	// Take in the "service" instance (in this case representing an asynchronous
	// server) and the completion queue "cq" used for asynchronous communication
	// with the gRPC runtime.
	CallData(MapReduceWorker::AsyncService*, ServerCompletionQueue*);
	void Proceed();

private:
	// The means of communication with the gRPC runtime for an asynchronous
	// server.
	MapReduceWorker::AsyncService* service_;
	// The producer-consumer queue where for asynchronous server notifications.
	ServerCompletionQueue* cq_;
	// Context for the rpc, allowing to tweak aspects of it such as the use
	// of compression, authentication, as well as to send metadata back to the
	// client.
	ServerContext ctx_;

	// What we get from the client.
	WorkerRequest request_;
	// What we send back to the client.
	WorkerReply reply_;

	// The means to get back to the client.
	ServerAsyncResponseWriter<WorkerReply> responder_;

	// Let's implement a tiny state machine with the following states.
	enum CallStatus { CREATE, PROCESS, FINISH };
	CallStatus status_;  // The current serving state.
};


CallData::CallData(MapReduceWorker::AsyncService* service, ServerCompletionQueue* cq)
		: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
	// Invoke the serving logic right away.
	Proceed();
}


void CallData::Proceed() {
	// Reference: https://github.com/grpc/grpc/blob/v1.33.2/examples/cpp/helloworld/greeter_async_server.cc
	if (status_ == CREATE) {
		// Make this instance progress to the PROCESS state.
		status_ = PROCESS;

		// As part of the initial CREATE state, we *request* that the system
		// start processing SayHello requests. In this request, "this" acts are
		// the tag uniquely identifying the request (so that different CallData
		// instances can serve different requests concurrently), in this case
		// the memory address of this CallData instance.
		cout << "[worker.h] INFO: Worker listening for requests" << endl;
		service_->RequestWorkerFn(&ctx_, &request_, &responder_, cq_, cq_,
								this);
	} else if (status_ == PROCESS) {
		// Spawn a new CallData instance to serve new clients while we process
		// the one for this CallData. The instance will deallocate itself as
		// part of its FINISH state.
		new CallData(service_, cq_);

		// The actual processing.
		bool job_type = request_.job_type();

		if (job_type == MAP) {
			cout << "[worker.h] INFO: Running a map job" << endl;
			/* Map logic begins here */
			// Get map function corresponding to the user id.
			shared_ptr<BaseMapper> map_fn = get_mapper_from_task_factory(request_.user_id());

			// Set the number of intermediate output files for the mapper (see mr_tasks.h)
			Worker::setNOutputFiles(map_fn, request_.n_output_files());

			// Set tmp directory.
			Worker::setTmpLoc(map_fn, request_.tmp_loc());

			// Iterate over the shard components that make up the shard.
			for (int i=0; i < request_.component_size(); i++) {
				// Get relevant data from request
				ShardComp shard_comp = request_.component(i);
				string filename = shard_comp.filename();
				streampos start = shard_comp.start();
				streampos end = shard_comp.end();
				int component_size = shard_comp.component_size();

				// Debug
				cout << "[worker.h] INFO: Map job running on shard component: " << endl\
				<< "\t Parent shard id: " << request_.shard_id() << endl\
				<< "\t Parent shard size: " << request_.shard_size() << endl\
				<< "\t Component filename: " << filename << endl\
				<< "\t Component size: " << component_size << endl\
				<< "\t Component Start: " << start << endl\
				<< "\t Component End: " << end << endl
				<< "--------------------------------------------------------------------------" << endl;
				
				// Read lines from file for corresponding shard component.
				ifstream ifs(filename, ios::binary);
				if (ifs.is_open()) {
					ifs.seekg(start, ios::beg);
					string line;
					while(ifs.tellg() < end && getline(ifs, line)) {
						map_fn->map(line);
					}
					ifs.close();
				}
				else {
					cerr << "[worker.h] ERROR: Cannot open shard input file: " << filename << endl;
					exit(1);
				}
			}
			// Shard has been processed. Construct the reply with the result intermediate files.
			cout << "[worker.h] INFO: Shard " << request_.shard_id() << " has been processed." << endl;
			unordered_set<string> intermediate_files = Worker::getIntermediateFiles(map_fn);
			for (const string& file : intermediate_files) {
				reply_.add_intermediate_files(file);
			}
			reply_.set_complete(true);
			
			// Map job complete.
			cout << "[worker.h] INFO: Map job complete!" << endl;
		}
		else {
			cout << "[worker.h] INFO: Running a reduce job..." << endl;
			// Reduce logic here
			// Get reuce function corresponding to the user id.
			shared_ptr<BaseReducer> reduce_fn = get_reducer_from_task_factory(request_.user_id());
			
			// Set number of output files.
			Worker::setNOutputFilesReducer(reduce_fn, request_.n_output_files());

			// Set output directory
			Worker::setOutputLoc(reduce_fn, request_.output_loc());

			// Read lines from intermediate file and call reduce on them.
			string filename = request_.tmp_loc();
			ifstream ifs(filename, ios::binary);

			// Data structure for getting input for reducer
			map<string, vector<string>> grouped_keys;

			if (ifs.is_open()) {
				string line;
				while (getline(ifs, line)) {
					istringstream linestream(line);
					string key;
					string value;

					linestream >> key;
					linestream >> value;

					grouped_keys[key].push_back(value);
				}
				ifs.close();
			}
			else {
				cerr << "[worker.h] (Reduce) ERROR: Cannot open tmp input file: " << filename << endl;
				exit(1);
			}

			// Call the actual reduce function with accumulated keys.
			for (auto& pair : grouped_keys) {
				reduce_fn->reduce(pair.first, pair.second);
			}
			
			// Mark reply as complete.
			reply_.set_complete(true);

			// Reduce job complete
			cout << "[worker.h] INFO: Reduce job complete!" << endl;
		}
		
		// And we are done! Let the gRPC runtime know we've finished, using the
		// memory address of this instance as the uniquely identifying tag for
		// the event.
		status_ = FINISH;
		responder_.Finish(reply_, Status::OK, this);
	} else {
		GPR_ASSERT(status_ == FINISH);
		// Once in the FINISH state, deallocate ourselves (CallData).
		delete this;
	}
}


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) : worker_addr_(ip_addr_port) {
	// Reference: https://github.com/grpc/grpc/blob/v1.33.2/examples/cpp/helloworld/greeter_async_server.cc
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    cout << "[worker.h]{" << ip_addr_port << "} INFO: Worker listening." << endl;
}


// This can be run in multiple threads if needed.
void Worker::Handler() {
	// Reference: https://github.com/grpc/grpc/blob/v1.33.2/examples/cpp/helloworld/greeter_async_server.cc
    // Spawn a new CallData instance to serve new clients.
    new CallData(&service_, cq_.get());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallData*>(tag)->Proceed();
	}
}


/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));

	Handler();

	return true;
}
