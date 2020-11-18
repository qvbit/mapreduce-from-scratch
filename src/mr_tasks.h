#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <mutex>
#include <unordered_set>
#include <unordered_map>

using namespace std;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string output_dir_;  // Output directory of files
		int n_output_files_;  // Corresponds to R (number of reduce tasks)
		mutex file_mutex_;  // Synchronize access to the file so multiple workers don't write at the same time.
		hash<string> string_hash_fn_;  // String hash fn fresh from the factory.
		unordered_set<string> intermediate_files_;  // Intermediate files output by mapper.
		unordered_map<string, string> hashkey_to_filepath_; // Contains mapping from hashed key to intermediate output loc.
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
	for (int i=0; i < n_output_files_; i++) {
		hashkey_to_filepath_[to_string(i)] = "output/intermediate" + to_string(i) + ".txt";
	}
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	// std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	// Hash the key so that we can distribute the keys randomly and evenly to R output files (by key)
	string hashed_key = to_string(string_hash_fn_(key) % n_output_files_);
	// Look up the filepath for this key.
	string filepath = hashkey_to_filepath_[hashed_key];
	// Open file with append mode.
	ofstream ofs(filepath, ios::app);

	if (ofs.is_open()) {
		ofs << key << " " << val << endl;
	}
	else {
		cerr << "[mr_tasks.h] ERROR: Failed to open file: " << filepath << endl;
		exit(1);
	}
	ofs.close();

	// Note that we need to do this step since we have no way of knowing apriori exactly 
	// which of the files will be written to. E.g. if n_output_files = R = 8 but the keys 
	// happen to hash to only values 1, 2 then 6 of the files will be unused and should not be
	// forwarded to the master.
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
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
