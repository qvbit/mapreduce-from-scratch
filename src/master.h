#pragma once

#include <grpc++/grpc++.h>
#include <string>
#include <iostream>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <chrono>

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

using namespace std;

enum WorkerState {AVAILABLE=1, BUSY=2};

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
		bool asyncReduce(const string&, const FileShard&);
		string getWorker();

		// Data members
		MapReduceSpec mr_spec_;
		vector<FileShard> file_shards_;
		vector<string> intermediate_files_; // Intermediate files output by worker
		ThreadPool *pool_;  // Same thread pool from last project
		map<string, WorkerState> worker_state_; // <worker_addr, AVAILABLE/BUSY>
		mutex mutex_worker_state_; // Mutex for synchronized access to worker_state_
		int tasks_left_; // Acts as barrier for all map jobs to finish before starting reduce.
		mutex mutex_main_; // Main mutex to be used in run()
		condition_variable cv_main_; // run() waits for signal from this cv before running reduce.
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
	cout << "[master.h] INFO: Master initialized." << endl;
}

inline string Master::getWorker() {
	for (auto& temp_addr : mr_spec_.worker_ipaddr_ports) {
		if (worker_state_[temp_addr] == AVAILABLE) {
			worker_state_[temp_addr] = BUSY;
			return temp_addr;
		}
	}
	return "-1";
}

bool Master::runMap() {
	tasks_left_ = file_shards_.size();
	vector<future<bool>> results;
	
	cout << "[master.h] INFO: Running map phase, tasks left: " << tasks_left_ << endl;
	cout << "[master.h] INFO: # Fileshards: " << file_shards_.size() << endl;
	for (int i=0; i < file_shards_.size(); i++) {
		// Assign each file shard to a new thread to handle. 
		results.emplace_back(
			pool_->queueTask([this, i] {
				// Keep looping until we find an available worker.
				string worker_addr = "-1";
				while (worker_addr == "-1") {
					mutex_worker_state_.lock();
					worker_addr = this->getWorker();
					mutex_worker_state_.unlock();
				}
				// We now have an available worker. Call the RPC.
				cout << "[master.h] INFO: (runMap) Worker addr:" << worker_addr << " assigned to shard: " << i << endl;
				bool rpc_res = this->asyncMap(worker_addr, this->file_shards_[i]);

				// Notify main so it can decrement count (this implements barrier).
				// this->cv_main_.notify_one();
				return rpc_res;
			})
		);
	}
    for(auto&& result : results) {
		cout << "Result!!" << endl;
        if (!result.get()) {
			return false;
		}
	}
	return true;
}


bool Master::asyncMap(const string& worker_addr, const FileShard& fileshard) {
	cout << "[master.h] INFO: (asyncMap) Worker addr:" << worker_addr << " is running now!" << endl;

	this_thread::sleep_for(chrono::seconds(2));

	worker_state_[worker_addr] = AVAILABLE;
	return true;
}



/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// NOTE: The below barrier implementation is correct because the MapReduce job only runs once.
	// If it ran more than once, this barrier may lead to race conditions.
	runMap();
	return true;
}