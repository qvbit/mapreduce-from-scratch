Implementation of the MapReduce framework from scratch in C++ using gRPC for communicaton between nodes. 

I referenced the async grpc (https://grpc.io/docs/languages/cpp/async/) documentation heavily to implement the gRPC boilerplate in worker.h and master.h.

Please symbolically link the input files so they are in the same directory as the binary. This is done in my cmakelists by default but it only considers testdata_1.txt, testdata_2.txt, testdata_3.txt. If you would like to add other test files, please add them in the camke configuration.

There is one additional header which is threadpool.h. This is my threadpool from PR3.

I wrote a couple unit tests which can be found in `testMapReduce.py`. If I had more time, I would flesh out a more comprehensive test suite.

