#include <vector>
#include <iostream>
#include <algorithm>
#include <queue>
#include <functional>
#include <map>

#include "moderndbs/external_sort.h"
#include "moderndbs/file.h"


namespace moderndbs {

    using my_pair_t = std::pair<int ,uint64_t>;

    void external_sort(File &input, size_t num_values, File &output, size_t mem_size) {
        /// If input size is 0 return
        if (input.size() == 0) {
            return;
        }
        /// Input file size
        size_t file_size = input.size();

        /// how many elements we can fit into main memory for each run
        int maxNumberOfElementsInMemory = mem_size / sizeof(uint64_t);

        /// From how many sorted lists are we going to load data into main memory for k-way merge
        /// We need k sorted lists
        /// In order to ceil an integer I used this calculation
        int numberOfRuns = 1 + ((num_values - 1) / maxNumberOfElementsInMemory);

        /// Resize output file by using input size
        output.resize(file_size);

        /// Create dataStorage for sorting, data from input file will be inserted into this vector ve k sorted runs
        std::vector<uint64_t> dataStorage;

        /// The offset in the file at which the block should be written.
        size_t offSet = 0;

        /// If offSet + mem_size is greater than file size, then use file size as block size.
        /// Because we have enough space in memory
        /// Otherwise use mem_size as block size and read data from file chunk by chunk
        if (offSet + mem_size >= file_size) {
            /// resize dataStorage to store data from file
            std::vector<uint64_t> dataStorage(num_values);

            /// Read block from input file
            input.read_block(offSet, file_size, reinterpret_cast<char *>(dataStorage.data()));

            /// Sort data
            std::sort(dataStorage.begin(), dataStorage.end());

            /// Write to output file
            output.write_block(
                    reinterpret_cast<char *>(dataStorage.data()),
                    offSet,
                    file_size
            );
        } else {
            /// Resize dataStorage by using maximum amount of elements that can fit into memory
            dataStorage.resize(maxNumberOfElementsInMemory);

            /// Double array to keep offsets of each block in temporary file
            /// This information will be used to track offset information for k sorted lists
            /// Since only one file will be used to store the k sorted lists
            /// When we do k-way merge of all runs, the data will be read by using this information
            /// Add a virtual offset to keep track of file ending offset
            /// Create a double array. array[i][j] i refers to list index (starting from 1)
            /// j refers to file offsets. j can only have either 0 or 1.
            /// 0 refers to starting offset in the file from which the block should be read.
            /// 1 refers to ending offset in the file for a that list (result of a run)
            int sizeOfBlocks[numberOfRuns + 2][2];
            sizeOfBlocks[0][0] = 0;
            sizeOfBlocks[0][1] = 0;
            int counter = 1;

            /// Create a temporary file to write data of k sorted lists
            const std::unique_ptr<File> &tempFile = moderndbs::File::make_temporary_file();
            auto *temporaryFile = reinterpret_cast<File *>(tempFile.get());

            /// Read data until we reach to end of file, offSet + mem_size should be smaller than input file size
            while (offSet + mem_size <= file_size) {
                /// Read from input file
                input.read_block(offSet, mem_size, reinterpret_cast<char *>(dataStorage.data()));

                /// Sort the data
                std::sort(dataStorage.begin(), dataStorage.end());

                /// Write the sorted data into a temporary output file
                temporaryFile->write_block(reinterpret_cast<char *>(dataStorage.data()), offSet, mem_size);

                /// Set starting offset for each sorted list
                sizeOfBlocks[counter][0] = offSet;
                sizeOfBlocks[counter][1] = 0;
                counter++;

                /// read the next chunk of data
                offSet += mem_size;
            }

            /// There might be still some data to be read in file
            /// Because mem_size + offSet can be greater than input file size
            size_t remaining = (file_size - offSet) / sizeof(uint64_t);

            /// if there are still some data to be read, read it, sort it and write it in the temporary output file
            if (remaining > 0) {
                /// first clear dataStorage to read new data
                dataStorage.clear();
                dataStorage.resize(remaining);

                input.read_block(offSet, remaining * sizeof(uint64_t), reinterpret_cast<char *>(dataStorage.data()));

                /// sort the remaining data
                std::sort(dataStorage.begin(), dataStorage.end());

                /// write the remaining data into a temporary output file
                temporaryFile->write_block(reinterpret_cast<char *>(dataStorage.data()),
                                           offSet, remaining * sizeof(uint64_t));

                /// Set starting offset for each sorted list
                sizeOfBlocks[counter][0] = offSet;
                sizeOfBlocks[counter][1] = 0;

                /// Create a virtual offset to control the end of input file
                sizeOfBlocks[counter + 1][0] = offSet + (remaining * sizeof(uint64_t));
                sizeOfBlocks[counter + 1][1] = 0;
            } else {
                /// Create a virtual offset to control the end of input file when we dont have remaining data
                sizeOfBlocks[counter + 1][0] = offSet;
                sizeOfBlocks[counter][1] = 0;
            }

            /// Clear dataStore, open space in memory
            dataStorage.clear();

            /// Use only half of memory to load data, other part will be used for sorting results
            size_t requiredMemoryToLoadData = (maxNumberOfElementsInMemory / 2) * sizeof(uint64_t);
            /// How much memory do we need to load data from all k sorted lists
            size_t requiredMemoryForEachList =  requiredMemoryToLoadData / numberOfRuns;
            size_t fileOffSet = 0;
            size_t outPutFileSOffSet = 0;

            /// Create a map with an integer key where value is a priority queue
            /// Key will be used to identify which lists are used
            /// Priority queue is to store list items
            std::map<int, std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<>>> lists;
            for(int i=1;i <= numberOfRuns; i++) {
                /// I actually wanted to load data directly into priority queue of each list
                /// However I couldn't manage that that's why I first load data into a list then
                /// Copy it into priority queue. This can be improved.
                std::vector<uint64_t> list;

                /// If we don't need to use the all of the available memory for each list
                /// Calculate actual need of memory
                size_t requiredMemForChunk = requiredMemoryForEachList;
                if (static_cast<size_t>(sizeOfBlocks[i + 1][0] - sizeOfBlocks[i][0]) < requiredMemoryForEachList) {
                    requiredMemForChunk = sizeOfBlocks[i + 1][0] - sizeOfBlocks[i][0];
                }
                list.resize(requiredMemForChunk / sizeof(uint64_t));

                /// Read first chunk of data from temporary output file
                temporaryFile->read_block(fileOffSet, requiredMemForChunk, reinterpret_cast<char *>(list.data()));

                std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<>> priorityQueue;
                for (uint64_t val : list) {
                    priorityQueue.push(val);
                }

                /// Set priority queue of the list
                lists[i] = priorityQueue;
                /// Since we read the first chunks of data, next time do not read starting from the same offset
                sizeOfBlocks[i][1] += requiredMemForChunk;
                fileOffSet += mem_size;
            }
            /// Custom comparator to find smallest number in priority queue
            auto my_comp = [](const my_pair_t& e1, const my_pair_t& e2) {
                return e1.second > e2.second;
            };

            /// Since we reserved the other part of memory for sorting, create another priority queue
            /// Add the first chunks of data and removed them from their corresponding lists
            std::priority_queue<my_pair_t, std::vector<my_pair_t>, decltype(my_comp)> pq(my_comp);
            for(int i=1;i <= numberOfRuns; i++) {
                pq.push(std::make_pair(i, lists[i].top()));
                lists[i].pop();
            }

            size_t totalReadData = 0;
            /// Start to sort until our priority queue is empty
            /// The idea is whenever we pop a data from priority queue
            /// We need to load new data into priority queue from the list of element that we just used
            while(!pq.empty()) {
                /// First control if we have enough space in our memory in order to push sorted data
                /// If yes, then write it into dataStorage
                /// Otherwise, write the first sorted chunk of data into main output file clear dataStorage
                /// And push the next minimum number into our cleared dataStorage
                if ((totalReadData + sizeof(uint64_t)) <= requiredMemoryToLoadData) {
                    dataStorage.push_back(pq.top().second);
                    totalReadData += sizeof(uint64_t);
                } else {
                    output.resize(outPutFileSOffSet + (dataStorage.size() * sizeof(uint64_t)));
                    output.write_block(reinterpret_cast<char *>(dataStorage.data()), outPutFileSOffSet, dataStorage.size() * sizeof(uint64_t));
                    outPutFileSOffSet += dataStorage.size() * sizeof(uint64_t);
                    dataStorage.clear();
                    totalReadData = 0;
                    dataStorage.push_back(pq.top().second);
                }

                /// If the corresponding list is not empty (because we just pushed a data into dataStorage)
                /// Read another value from the list, pq.top().first returns the list id
                if (!lists[pq.top().first].empty()) {
                    /// If we have still data to read, push it into our priority queue for sorting
                    /// Remove the data from the corresponding list
                    if (lists[pq.top().first].top() != 0) {
                        pq.push(std::make_pair(pq.top().first, lists[pq.top().first].top()));
                        lists[pq.top().first].pop();
                    }
                    /// Check if the list is empty
                    if (lists[pq.top().first].empty()) {
                        /// If yes, calculate how much memory we need to read it. sizeOfBlocks is important because
                        /// It allows us to track offset for each list
                        /// In order to find how many data we will read
                        /// We substract the current offset from the next list starting offset then check
                        size_t requiredMem = requiredMemoryForEachList;
                        if (static_cast<size_t>(sizeOfBlocks[pq.top().first + 1][0] - (sizeOfBlocks[pq.top().first][0] + sizeOfBlocks[pq.top().first][1]))  < requiredMemoryForEachList) {
                            requiredMem = sizeOfBlocks[pq.top().first + 1][0] - (sizeOfBlocks[pq.top().first][0] + sizeOfBlocks[pq.top().first][1]);
                        }
                        /// If we have still some data that we can read
                        if (requiredMem != 0) {
                            /// Read data and set the correct offset for that specific list which we just used
                            std::vector<uint64_t> list;
                            list.resize(requiredMem / sizeof(uint64_t));
                            size_t offSetForChunk = sizeOfBlocks[pq.top().first][0] + sizeOfBlocks[pq.top().first][1];
                            if ((offSetForChunk + requiredMem) <= static_cast<size_t>(sizeOfBlocks[pq.top().first + 1][0])) {
                                temporaryFile->read_block(offSetForChunk,
                                                          requiredMem,
                                                          reinterpret_cast<char *>(list.data())
                                );
                                sizeOfBlocks[pq.top().first][1] += requiredMem;
                                std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<>> priorityQueue;
                                for (uint64_t val : list) {
                                    priorityQueue.push(val);
                                }
                                /// Insert data into priority queue of the list, so that in the next step
                                /// we can use this information
                                lists[pq.top().first] = priorityQueue;
                            }
                        }
                    }
                }
                /// Remove the data we read from priority queue
                pq.pop();
            }
            /// Clear lists to free memory
            lists.clear();
            /// If we have still data in dataStorage since while loop can be exited before writing all the data
            /// Write the data into main output file.
            if (dataStorage.size() > 0) {
                output.resize(outPutFileSOffSet + (dataStorage.size() * sizeof(uint64_t)));
                output.write_block(reinterpret_cast<char *>(dataStorage.data()), outPutFileSOffSet, dataStorage.size() * sizeof(uint64_t));
                /// Clear dataStorage
                dataStorage.clear();
            }
        }
    }
}  // namespace moderndbs
