package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// Create a hash table to store the values that have the same key name
	keyValueMap := make(map[string][]string)

	// Read and decode the data from the intermediate file, then store in the hash table
	for mapTask := 0; mapTask < nMap; mapTask++ {
		filename := reduceName(jobName, mapTask, reduceTask)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("Can't open the file: ", filename)
		}
		defer file.Close()

		// func Decode() store the value in the value pointed to by keyValuePair
		decoder := json.NewDecoder(file)
		var keyValuePair KeyValue
		for decoder.More() {
			err := decoder.Decode(&keyValuePair)
			if err != nil {
				log.Fatal("Decode failed")
			}
			// one by one store the value that has same key in the hash table
			keyValueMap[keyValuePair.Key] = append(keyValueMap[keyValuePair.Key], keyValuePair.Value)
		}
	}

	// sort the key in increasing order
	// keys is a slice to store
	keys := make([]string, 0, len(keyValueMap))
	for k := range keyValueMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// create the outfile, remember to close the file
	f, err := os.Create(outFile)
	if err != nil {
		log.Fatal("Can't create the outfile: ", outFile)
	}
	defer f.Close()

	// use json.NewEncoder() to write and encode the data into the outfile
	// func reduce() is used to deal with values that have the same key
	enc := json.NewEncoder(f)
	for _, k := range keys {
		_ = enc.Encode(KeyValue{k, reduceF(k, keyValueMap[k])})
	}
}
