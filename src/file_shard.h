#pragma once

#include <vector>
#include <math.h>
#include <limits>
#include <fstream>
#include <iostream>
#include "mapreduce_spec.h"

#define CONVERSION 1024.0

using namespace std;

// Compnents within the FileShard struct.
struct ShardComponent {
     string filepath;
     streampos start;
     streampos end;
     int component_size;
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     int shard_id;
     int shard_size;
     // TODO: Would be effecient to have vector of pointers to struct rather than struct.
     vector<ShardComponent> shard_components;
};

inline streamsize calc_filesize(const string& filepath) {
     ifstream ifs(filepath, ios::ate | ios::binary);
     if (!ifs.is_open()) {
          cerr << "[file_shard.h] ERROR: Unable to find file." << endl;
     }
     streamsize fsize = ifs.tellg();
     ifs.close();

     cout << "[file_shard.h] INFO: File size: " << (fsize / CONVERSION) << endl;
     return fsize;
}

inline void display_shard_component(const ShardComponent& shard_component) {
     cout << "Shard component info:" << endl;
     cout << "\t Filepath: " << shard_component.filepath << endl;
     cout << "\t Start: " << shard_component.start << endl;
     cout << "\t End: " << shard_component.end << endl;
}

inline void display_file_shard(const FileShard& file_shard) {
     cout << "--------------------File Shard id: " << file_shard.shard_id << "------------------------" << endl;
     cout << "File Shard id: " << file_shard.shard_id << endl;
     cout << "Shard size: " << (file_shard.shard_size / CONVERSION) << endl;
     
     for (auto const& shard_component : file_shard.shard_components) {
          display_shard_component(shard_component);
     }
     cout << "------------------------------------------------------------" << endl;
}

inline void display_all_file_shards(const vector<FileShard>& fileShards) {
     for (auto const& shard : fileShards) {
          display_file_shard(shard);
     }
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, vector<FileShard>& fileShards) {
     int shard_id = 0;
     // Calculate the number of shards (M)
     streamsize total_fsize = 0;
     for (auto const& filepath : mr_spec.input_files) {
          cout << "[file_shard.h] INFO: Calculating size for: " << filepath << endl;
          total_fsize += calc_filesize(filepath);
     }
     cout << "[file_shard.h] INFO: Total fsize: " << (total_fsize / CONVERSION) << endl;
     total_fsize /= CONVERSION; // Convert to KB
     int m_shards =  (int) ceil(total_fsize / mr_spec.map_kilobytes);

     // Shard the data
     for (auto const& filepath : mr_spec.input_files) {
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
                    // Space left in previous shard
                    if (prev_file_shard.shard_size < (mr_spec.map_kilobytes * CONVERSION)) {
                         map_size = mr_spec.map_kilobytes - floor(prev_file_shard.shard_size / CONVERSION);
                         fileShards.pop_back(); // Get rid of this, we'll update it and put it back.
                         file_shard = prev_file_shard;
                    }
                    else { // Else create new shard
                         shard_id += 1;
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
               ShardComponent shard_component;
               shard_component.filepath = filepath;
               shard_component.start = start;
               shard_component.end = end;
               shard_component.component_size = block_size;

               // Add this shard component to the full file shard.
               file_shard.shard_size += block_size;
               file_shard.shard_id = shard_id;
               file_shard.shard_components.push_back(shard_component);

               // Push this file shard to fileShards (NOTE: file_shard may not be done yet).
               fileShards.push_back(file_shard);
          }
          ifs.close();
     }
     // Display the file shards for debugging purposes:
     display_all_file_shards(fileShards);
     return true;
}
