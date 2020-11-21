#pragma once

#include <string>
#include <iostream>
#include <fstream>
// #include <experimental/filesystem>
#include <mutex>
#include <unordered_set>
#include <unordered_map>

using namespace std;
// namespace fs = experimental::filesystem;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string tmp_loc_;  // Output directory of files
		int n_output_files_;  // Corresponds to R (number of reduce tasks)
		mutex file_mutex_;  // Synchronize access to the file so multiple workers don't write at the same time.
		hash<string> string_hash_fn_;  // String hash fn fresh from the factory.
		unordered_set<string> intermediate_files_;  // Intermediate files output by mapper.
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	// std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	// Hash the key so that we can distribute the keys randomly and evenly to R output files (by key)
	string hashed_key = to_string(string_hash_fn_(key) % n_output_files_);
	// Look up the filepath for this key.
	string filepath = tmp_loc_ + "/intermediate" + hashed_key + ".txt";
	// cout << "[mr_tasks.h] INFO: Intermediate filepath is: " << filepath << endl;

	// Critical section to write to file
	lock_guard<mutex> lock(file_mutex_);

	ofstream ofs(filepath, ios::app);

	// Open file with append mode.
	if (ofs.is_open()) {
		ofs << key << " " << val << endl;
	}
	else {
		cerr << "[mr_tasks.h] ERROR: Unable to open file: " << filepath << endl;
		exit(1);
	}
	ofs.close();

	intermediate_files_.insert(filepath);
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	// std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
