#pragma once

#include <string>
#include <stdio.h>
#include <iostream>
#include <vector>
#include <unordered_set>
#include <map>
#include <thread>
#include <mutex>
#include <chrono>
#include <sys/stat.h>
#include <assert.h>
#include <grpc++/grpc++.h>

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "threadpool.h"
#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::Status;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using masterworker::MapReduceWorker;
using masterworker::WorkerRequest;
using masterworker::WorkerReply;
using masterworker::ShardComp;

using namespace std;

enum WorkerState {AVAILABLE=1, BUSY=2};
enum JobType {MAP=0, REDUCE=1};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {
	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		// Member functions
		bool runMap();
		bool runReduce();
		bool asyncMap(const string&, const FileShard&);
		bool asyncReduce(const string&, const string&);
		string getWorker();

		// Data members
		MapReduceSpec mr_spec_;
		vector<FileShard> file_shards_;
		unordered_set<string> intermediate_files_; // Intermediate files output by worker
		ThreadPool *pool_;  // Same thread pool from last project
		map<string, WorkerState> worker_state_; // <worker_addr, AVAILABLE/BUSY>
		mutex mutex_worker_state_; // Mutex for synchronized access to worker_state_
		string tmp_loc_;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	pool_ = new ThreadPool(mr_spec.n_workers);
	file_shards_ = file_shards;
	mr_spec_ = mr_spec;
	
	// All workers initially available.
	for (auto& worker_ipaddr_port : mr_spec.worker_ipaddr_ports) {
		worker_state_[worker_ipaddr_port] = AVAILABLE;
	}

	// Set tmp directory (hardcoded for now).
	tmp_loc_ = "tmp";

	cout << "[master.h] INFO: Master initialized." << endl;
}

bool Master::runMap() {
	vector<future<bool>> results;
	
	cout << "[master.h] INFO: # Fileshards for Mapper: " << file_shards_.size() << endl;
	for (int i=0; i < file_shards_.size(); i++) {
		// Assign each file shard to a new thread to handle. 
		results.emplace_back(
			pool_->queueTask([this, i] {
				// Keep looping until we find an available worker.
				string worker_addr = "-1";
				while (worker_addr == "-1") {
					// Critical section
					lock_guard<mutex> lock(this->mutex_worker_state_);
					worker_addr = this->getWorker();
				}
				// We now have an available worker. Call the RPC.
				cout << "[master.h] INFO: (runMap) Worker addr:" << worker_addr << " assigned to shard: " << i << endl;
				bool rpc_res = this->asyncMap(worker_addr, this->file_shards_[i]);

				return rpc_res;
			})
		);
	}
	// This blocks until all mappers are done effectively implementing a barrier before the reduce.
    for(auto&& result : results) {
		cout << "Result!!" << endl;
        if (!result.get()) {
			return false;
		}
	}
	return true;
}

// Reference for grpc boilerplate: https://grpc.io/docs/languages/cpp/async/
bool Master::asyncMap(const string& worker_addr, const FileShard& fileshard) {
	cout << "[master.h] INFO: (asyncMap) Worker addr:" << worker_addr << " is running now!" << endl;

	// Create temp directory if it does not already exist.
	mkdir(tmp_loc_.c_str(), S_IRWXU);

	// Create stub
	unique_ptr<MapReduceWorker::Stub> stub = MapReduceWorker::NewStub(
		grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials())
	);

	// Create the request message
	WorkerRequest request;
	request.set_user_id(mr_spec_.user_id);
	request.set_shard_id(fileshard.shard_id);
	request.set_n_output_files(mr_spec_.n_output_files);
	request.set_tmp_loc(tmp_loc_);
	request.set_job_type(MAP);
	request.set_shard_size(fileshard.shard_size);

	for (auto& shard_component : fileshard.shard_components) {
		ShardComp* shard_comp = request.add_component();
		shard_comp->set_filename(shard_component.filepath);
		shard_comp->set_start(shard_component.start);
		shard_comp->set_end(shard_component.end);
		shard_comp->set_component_size(shard_component.component_size);
	}

	// Initialize the RPC call and create handle (rpc). Also bind it to cq.
	ClientContext context;
	CompletionQueue cq;
	unique_ptr<ClientAsyncResponseReader<WorkerReply>> rpc(
		stub->AsyncWorkerFn(&context, request, &cq)
	);

	// Ask for the reply and final status
	WorkerReply reply;
	Status status;
	rpc->Finish(&reply, &status, (void*)1);

	// Wait for cq to return the next tag. The reply and status are ready once the tag 
	// passed into the corresponding Finish() call is returned.
	void* got_tag;
	bool ok = false;
	GPR_ASSERT(cq.Next(&got_tag, &ok));
	GPR_ASSERT(ok && got_tag == (void*)1);

	if (!status.ok()) {
		cout << "[master.h] ERROR: " << status.error_code() << " - " << status.error_message() << endl;
		return false;
	}

	// Make sure job actually completed. 
	// assert(reply.complete());

	// Save temporary file(s) to set. 
	for (int i=0; i < reply.intermediate_files_size(); i++) {
		intermediate_files_.insert(reply.intermediate_files(i));
	}

	// Make worker available for work.
	worker_state_[worker_addr] = AVAILABLE;
	
	return true;
}


bool Master::runReduce() {
	vector<future<bool>> results;

	cout << "[master.h] INFO: # Intermediate files for Reducer: " << intermediate_files_.size() << endl;
	for (const auto& file : intermediate_files_) {
		results.emplace_back(
			pool_->queueTask([this, file] {
				string worker_addr = "-1";
				while (worker_addr == "-1") {
					lock_guard<mutex> lock(this->mutex_worker_state_);
					worker_addr = this->getWorker();
				}
				cout << "[master.h] INFO: (runReduce) Worker addr:" << worker_addr << " assigned to file: " << file << endl;
				bool rpc_res = this->asyncReduce(worker_addr, file);

				return rpc_res;
			})
		);
	}
	// This blocks until all mappers are done effectively implementing a barrier before the reduce.
    for(auto&& result : results) {
		cout << "Reduce result!!" << endl;
        if (!result.get()) {
			return false;
		}
	}
	return true;
}

// Reference for grpc boilerplate: https://grpc.io/docs/languages/cpp/async/
bool Master::asyncReduce(const string& worker_addr, const string& filepath) {
	cout << "[master.h] INFO: (asyncReduce) Worker addr:" << worker_addr << " is running now!" << endl;

	// Create output directory if it does not already exist.
	mkdir(mr_spec_.output_dir.c_str(), S_IRWXU);

	// Create stub
	unique_ptr<MapReduceWorker::Stub> stub = MapReduceWorker::NewStub(
		grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials())
	);

	// Create the message
	WorkerRequest request;
	request.set_user_id(mr_spec_.user_id);
	request.set_n_output_files(mr_spec_.n_output_files);
	request.set_tmp_loc(filepath);
	request.set_output_loc(mr_spec_.output_dir);
	request.set_job_type(REDUCE);

	// Initialize the RPC call and create handle (rpc). Also bind it to cq.
	ClientContext context;
	CompletionQueue cq;
	unique_ptr<ClientAsyncResponseReader<WorkerReply>> rpc(
		stub->AsyncWorkerFn(&context, request, &cq)
	);

	// Ask for the reply and final status
	WorkerReply reply;
	Status status;
	rpc->Finish(&reply, &status, (void*)1);

	// Wait for cq to return the next tag. The reply and status are ready once the tag 
	// passed into the corresponding Finish() call is returned.
	void* got_tag;
	bool ok = false;
	GPR_ASSERT(cq.Next(&got_tag, &ok));
	GPR_ASSERT(ok && got_tag == (void*)1);

	if (!status.ok()) {
		cout << "[master.h] ERROR: " << status.error_code() << " - " << status.error_message() << endl;
		return false;
	}
	
	// Make sure job actually completed. 
	// assert(reply.complete());

	// Make worker available.
	worker_state_[worker_addr] = AVAILABLE;
	return true;
}


// Helper to get available worker.
inline string Master::getWorker() {
	for (auto& temp_addr : mr_spec_.worker_ipaddr_ports) {
		if (worker_state_[temp_addr] == AVAILABLE) {
			worker_state_[temp_addr] = BUSY;
			return temp_addr;
		}
	}
	return "-1";
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	/* --------------------------- MAP --------------------------- */
	assert(runMap());
	cout << "[master.h] INFO: Map job complete!" << endl;
	cout << "[master.h] INFO: All temp files output by Mappers: " << endl;
	for (const auto& elem : intermediate_files_) {
		cout << "\t" << elem << endl;
	}


	/* -------------------------- REDUCE -------------------------- */
	assert(runReduce());
	cout << "[master.h] INFO: Full MapReduce job is complete!!! Master exiting..." << endl;

	// Job complete: remove temp directory.
	for (const auto& tmp_file : intermediate_files_) {
		if (remove(tmp_file.c_str()) != 0)
			cout << "[master.h] WARNING: Failed to delete temp file.";
	}

	return true;
}