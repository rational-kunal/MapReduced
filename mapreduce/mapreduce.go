package mapreduce

import (
	"os"
	"log"
	"bufio"
	"sync"
)

type MapFuncPrototype func(string)
type ReduceFuncPrototype func(string, []string)

type MapReduce struct {
	InputFilename string
	
	IntermediateKV map[string][]string
	OutputKV map[string]string

	MapFunc MapFuncPrototype
	ReduceFunc ReduceFuncPrototype
}

func (mapReduce MapReduce) partionInputFile(inputData chan string) {
	file, err := os.Open(mapReduce.InputFilename)
	if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        inputData <- scanner.Text()
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
	}
	
	close(inputData)
}

func (mapReduce MapReduce) mapper(inputData chan string) {
	var wait sync.WaitGroup
	for inputLine := range inputData {
		wait.Add(1)
		go func (line string) {
			defer wait.Done()
			mapReduce.MapFunc(line)
		} (inputLine)
	}

	wait.Wait()
}

var m sync.Mutex
func (mapReduce MapReduce) EmitIntermediateKV(key string, value string) {
	m.Lock()
	mapReduce.IntermediateKV[key] = append(mapReduce.IntermediateKV[key], value)
	m.Unlock()
}

func (mapReduce MapReduce) reducer() {
	var wait sync.WaitGroup
	for key, value := range mapReduce.IntermediateKV {
		wait.Add(1)
		go func (key string, value []string) {
			defer wait.Done()
			mapReduce.ReduceFunc(key, value)
		} (key, value)
	}

	wait.Wait()
}

var mo sync.Mutex
func (mapReduce MapReduce) EmitOutputKV(key string, value string) {
	mo.Lock()
	mapReduce.OutputKV[key] = value
	mo.Unlock()
}

func (mapReduce MapReduce) Run() {
	inputData := make(chan string, 10)

	go mapReduce.partionInputFile(inputData)

	mapReduce.mapper(inputData)

	mapReduce.reducer()
}