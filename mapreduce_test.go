package mapreduce

import (
	"testing"
	"strconv"
	"reflect"
)

const INPUT_FILENAME = "./text.txt"
func TestWordCount(t *testing.T) {
	expectedOut := make(map[string]string)
	expectedOut["a"] = "10"
	expectedOut["b"] = "10"
	expectedOut["c"] = "10"
	expectedOut["e"] = "10"
	expectedOut["r"] = "10"
	expectedOut["t"] = "10"
	expectedOut["w"] = "10"

    mr := MapReduce{
		InputFilename: INPUT_FILENAME,
		IntermediateKV: make(map[string][]string),
		OutputKV: make(map[string]string),
	}
	
	mr.MapFunc = func(x string) {
		for _, char := range x {
			mr.EmitIntermediateKV(string(char), "1")
		}
	}

	mr.ReduceFunc = func(key string, x []string) {
		mr.EmitOutputKV(key, strconv.Itoa(len(x)))
	}

	mr.Run()

	if !reflect.DeepEqual(mr.OutputKV, expectedOut) {
		t.Error("Results are wrong")
		t.Error("MapReuce Result", mr.OutputKV)
		t.Error("Actuall Result", expectedOut)
	}
}