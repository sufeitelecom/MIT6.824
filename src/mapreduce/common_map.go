package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
	//
	// There is one intermediate file per reduce task. The file name
	// includes both the map task number and the reduce task number. Use
	// the filename generated by reduceName(jobName, mapTask, r)
	// as the intermediate file for reduce task r. Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	// Your code here (Part I).
	//
	//第一步，读写输入文件内容，并将其保存在data中
	file, err := os.Open(inFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		panic(err)
	}

	data := make([]byte, fileinfo.Size())
	_, err = file.Read(data)
	if err != nil {
		panic(err)
	}
	//第二步，使用用户指定map函数，将文件处理成kv键值对
	KeyValue := mapF(inFile, string(data))
	//第三步，构建中间文件，更新key的ihash值，将键值对以json格式写入中间文件
	for r := 0; r < nReduce; r++ {
		filename := reduceName(jobName, mapTask, r) //获取中间文件名
		reducefile, err := os.Create(filename)
		if err != nil {
			panic(err)
		}
		defer reducefile.Close()

		enc := json.NewEncoder(reducefile) //使用json格式
		for _, kv := range KeyValue {
			if ihash(kv.Key)%int(nReduce) == int(r) {
				err := enc.Encode(&kv)
				if err != nil {
					panic(err)
				}
			}
		}

	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
