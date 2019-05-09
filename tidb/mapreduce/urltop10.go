package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	// 第一阶段应当得出 url -> cnt 的结果
	args = append(args, RoundArgs{
		MapFunc:    ExampleURLCountMap1,
		ReduceFunc: ExampleURLCountReduce1,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	// 第二阶段直接在 map 阶段就可以得到每个文件的前十
	// 在 reduce 阶段直接合并结果即可
	args = append(args, RoundArgs{
		MapFunc:    ExampleURLTop10Map1,
		ReduceFunc: ExampleURLTop10Reduce1,
		NReduce:    1,
	})
	return args
}

// ExampleURLCountMap1 is the map function in the first round
func ExampleURLCountMap1(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvs = append(kvs, KeyValue{Key: l})
	}
	return kvs
}

// ExampleURLCountReduce1 is the reduce function in the first round
func ExampleURLCountReduce1(key string, values []string) string {
	return key + " " + strconv.Itoa(len(values)) + "\n"
}

// ExampleURLTop10Map1 is the map function in the first round
func ExampleURLTop10Map1(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")

	// get the top 10 of this file
	url2cnt := make(map[string]int, len(lines))
	for _, l := range lines {
		tmp := strings.Split(l, " ")
		if len(tmp) < 2 {
			continue
		}
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		url2cnt[tmp[0]] = n
	}

	urlList, cntList := TopN(url2cnt, 10)

	kvList := []KeyValue{}
	for i := range urlList {
		kv := KeyValue{Value: urlList[i] + " " + strconv.Itoa(cntList[i])}
		kvList = append(kvList, kv)
	}

	//fmt.Println("kvList:", kvList)
	return kvList
}

// ExampleURLTop10Reduce1 is the reduce function in the second reound
func ExampleURLTop10Reduce1(key string, values []string) string {
	//fmt.Println("values", values)
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
