#pragma once

#include <vector>
#include <math.h>
#include <limits>
#include <fstream>
#include <iostream>
#include "mapreduce_spec.h"

#define CONVERSION 1024.0

using namespace std;

struct ShardComponent {
     string filepath;
     streampos start;
     streampos end;
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     int shard_size;
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
     for (auto& filepath : mr_spec.input_files) {
          streamsize fsize = calc_filesize(filepath);
          streamsize fsize_remain = fsize;
          
          // Binary option to read /n character as-is.
          ifstream ifs(filepath, ios::binary);

          while (fsize_remain > 0) {
               int block_size;
               FileShard file_shard;
               streamsize map_size;

               // Check if old fileshard still has space left
               if (!fileShards.empty()) {
                    FileShard prev_file_shard = fileShards.back();
                    streamsize space_left = prev_file_shard.shard_size - (mr_spec.map_kilobytes * CONVERSION);
                    // Space left in previous shard
                    if (space_left > 0) {
                         map_size = space_left;
                         fileShards.pop_back(); // Get rid of this, we'll update it and put it back.
                         file_shard = prev_file_shard;
                    }
                    else { // Else create new shard
                         map_size = mr_spec.map_kilobytes;
                         file_shard.shard_size = 0;
                    }
               }
               else { // First element always creates new shard
                    map_size = mr_spec.map_kilobytes;
                    file_shard.shard_size = 0;
               }

               // Get start position
               streampos start = ifs.tellg();
               // Seek until we hit either the correct spot or eof.
               // cur specifies offset relative to current position.
               ifs.seekg(map_size*CONVERSION, ios::cur);
               streampos end = ifs.tellg();
               if (end > fsize) {
                    ifs.seekg(0, ios::end); // Go to end of file
                    end = ifs.tellg();
                    block_size = (end - start + 1);
               }
               else {
                    ifs.ignore(numeric_limits<streamsize>::max(), '\n'); // Move seek to the next /n character
                    ifs.seekg(-2, ios::cur); // Don't include /n character
                    end = ifs.tellg(); // Get end position BEFORE /n (don't want to pass \n to mapper).
                    block_size = (end - start + 2); // Count the newline character.
                    ifs.seekg(2, ios::cur); // Return seek to correct position after \n
               }
               fsize_remain -= block_size;

               // Create shard component for this block we just read.
               ShardComponent* shard_component = new ShardComponent;
               shard_component->filepath = filepath;
               shard_component->start = start;
               shard_component->end = end;

               // Add this shard component to the full file shard.
               file_shard.shard_size += block_size;
               file_shard.shard_components.push_back(shard_component);

               // Push this file shard to fileShards (NOTE: file_shard may not be done yet).
               fileShards.push_back(file_shard);
          }
          ifs.close();
     }
     return true;
}
