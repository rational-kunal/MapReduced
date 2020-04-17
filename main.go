package main

import (
	"./mapreduce"
)

const INPUT_FILENAME = "./text.txt"

func main() {
	mr := mapreduce.MapReduce{
		InputFilename: INPUT_FILENAME,
		IntermediateKV: make(map[string][]string),
		OutputKV: make(map[string]string),
	}

	mr.MapFunc = func(x string) {
		mr.EmitIntermediateKV(x, "1")
	}

	mr.ReduceFunc = func(key string, x []string) {
		mr.EmitOutputKV(key, string(len(x)))
	}

	mr.Run()

	// fmt.Println(mr.OutputKV)

}