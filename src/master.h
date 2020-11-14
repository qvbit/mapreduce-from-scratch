#pragma once

#include <grpc++/grpc++.h>
#include <string>
#include <iostream>
#include <vector>
#include <map>
#include <thread>

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

#define CONN_TIMEOUT 10 // in ms
#define RPC_TIMEOUT 2 // in s

using grpc::Channel;
using grpc::Status;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using masterworker::MapReduceService;
using masterworker::MapIn;
using masterworker::MapOut;
using masterworker::ReduceOut;
using masterworker::ShardMessage;

enum WorkerState {
	AVAILABLE=1,
	UNAVAILABLE=2
};

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
		// Base class that holds relevant state from worker in general.

		

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {

}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	return true;
}