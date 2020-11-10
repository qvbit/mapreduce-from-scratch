#pragma once

#include <vector>
#include <math.h>
#include <fstream>
#include "mapreduce_spec.h"

#define CONVERSION 1024

using namespace std;

struct ShardComponent {
     string filepath;
     streampos start;
     streampos end;
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     vector<ShardComponent*> shard_components;
};

// Reference: https://stackoverflow.com/questions/2409504/using-c-filestreams-fstream-how-can-you-determine-the-size-of-a-file
inline streamsize calc_filesize(const string& filepath) {
     ifstream ifs(filepath, ios::ate | ios::binary);
     streamsize fsize = ifs.tellg();
     ifs.close();
     return fsize;
}


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, vector<FileShard>& fileShards) {
     // Calculate the number of shards (M)
     streamsize total_fsize = 0;
     for (auto& filepath : mr_spec.input_files) {
          total_fsize += calc_filesize(filepath);
     }
     total_fsize /= CONVERSION; // Convert to KB
     int m_shards =  (int) ceil(total_fsize / mr_spec.map_kilobytes);

     // Shard the data
     
	
     return true;
}
