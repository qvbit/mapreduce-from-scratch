#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <assert.h>
#include <map>

using namespace std;

typedef map<string, vector<string>> ConfigIni;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;						// The number of mapreduce workers.
	vector<string> worker_ipaddr_ports;	// The addressed the workers are listening on.
	vector<string> input_files;			// These are the files that must be sharded.
	string output_dir;					// Where the output goes after job completion.
	int n_output_files;					// This corresponds to R from the paper.
	int map_kilobytes;					// The size of a shard.
	string user_id;						// Identifier of mapreduce job. 
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const string& config_filename, MapReduceSpec& mr_spec) {
	ifstream ifs(config_filename);
	ConfigIni config_map;
	string line;

	if (!ifs.is_open()) {
		cerr << "[mapreduce_spec.h] ERROR: Not able to find file" << endl;
		return false;
	}

	// Read in config into a map<string key, vector<string> values>
	while(getline(ifs, line)) {
		istringstream iss_outer (line);
		string key;
		if (getline(iss_outer, key, '=')) {
			string values;
			if (getline(iss_outer, values)) {
				vector<string> values_vector;
				istringstream iss_inner (values);
				while (iss_inner) {
					string value;
					if (getline(iss_inner, value, ',')) {
						values_vector.push_back(value);
					}
				}
				config_map[key] = values_vector;
			}
		}
	}
	ifs.close();

	// Read in the populated map into our structure.
	mr_spec.map_kilobytes = stoi(config_map["map_kilobytes"][0]);
	mr_spec.n_output_files = stoi(config_map["n_output_files"][0]);
	mr_spec.n_workers = stoi(config_map["n_workers"][0]);
	mr_spec.input_files = config_map["input_files"];
	mr_spec.worker_ipaddr_ports = config_map["worker_ipaddr_ports"];
	mr_spec.user_id = config_map["user_id"][0];
	mr_spec.output_dir = config_map["output_dir"][0];

	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {

	// Helper to print vector elements.
	auto print_vector = [](vector<string> const &input) {
		for (auto const& i : input) {
			cout << "\t" << i << endl;
		}
	};

	// Validate n_workers
	if (mr_spec.n_workers <= 0) {
		cerr << "[mapreduce_spec.h] ERROR: Number of workers must be positive" << endl;
		exit(1);
	}
	if (mr_spec.n_workers != mr_spec.worker_ipaddr_ports.size()) {
		cerr << "[mapreduce_spec.h] ERROR: Number of workers must match worker_addr list" << endl;
		exit(1);
	}
	
	// Validate worker_ipaddr_ports
	for (auto& ip : mr_spec.worker_ipaddr_ports) {
		size_t pos = ip.find(":");
		string host = ip.substr(0, pos);
		string port = ip.substr(pos+1);
		
		if(host.compare("localhost") != 0) {
			cerr << "[mapreduce_spec.h] ERROR: Host must be localhost, not " << host << endl;
			exit(1);
		}
		if ( !(stoi(port) >= 0 && stoi(port) <= 65535) ) {
			cerr << "[mapreduce_spec.h] ERROR: Not a valid TCP/IP port, must be 0 <= port <= 65535" << endl;
			exit(1);
		}
	}

	// Validate input files
	for (auto& file : mr_spec.input_files) {
		ifstream ifs(file);
		if (!ifs.is_open()) {
			cerr << "[mapreduce_spec.h] ERROR: Unable to open input file. Please make sure it was symbolically linked properly" << endl;
			exit(1);
		}
		ifs.close();
	}

	// Validate output file
	if(mr_spec.output_dir.empty()) {
		cerr << "[mapreduce_spec.h] ERROR: No output_dir was specified." << endl;
		exit(1);
	}

	// Validate n_output_files
	if (mr_spec.n_output_files <= 0) {
		cerr << "[mapreduce_spec.h] ERROR: n_output_files must be a positive number." << endl;
		exit(1);
	}

	// Validate map_kilobytes
	if (mr_spec.map_kilobytes <= 0) {
		cerr << "[mapreduce_spec.h] ERROR: map_kilobytes must be greater than 0." << endl;
		exit(1);
	}

	// Validate user_id
	if (mr_spec.user_id.empty()) {
		cerr << "[mapreduce_spec.h] ERROR: No user_id specified; you must specify one!" << endl;
		exit(1);
	}

	// Print out mr_spec
	cout << "----------------- Displaying Mr. Spec -----------------" << endl;
	cout << "n_workers: " << mr_spec.n_workers << endl;
	cout << "worker_ipaddr_ports: " << endl;
	print_vector(mr_spec.worker_ipaddr_ports);
	cout << "input_files: " << endl;
	print_vector(mr_spec.input_files);
	cout << "output_dir: " << mr_spec.output_dir << endl;
	cout << "n_output_files: " << mr_spec.n_output_files << endl;
	cout << "map_kilobytes: " << mr_spec.map_kilobytes << endl;
	cout << "user_id: " << mr_spec.user_id << endl;
	cout << "------------------- End of Mr. Spec -------------------" << endl;
	
	return true;
}
