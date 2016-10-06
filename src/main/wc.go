package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"
import "unicode"
import "strings"
import "strconv"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
  relist := list.New()
  slicefun := func(c rune) bool {
    return !unicode.IsLetter(c)
  }
  slices := strings.FieldsFunc(value, slicefun)
  for _, element := range slices {
    var pair mapreduce.KeyValue
    pair.Value = "1"
    pair.Key = element
    relist.PushBack(pair)
  }
  return relist
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
  result := 0
  num := 0
  for element := values.Front(); element != nil; element = element.Next() {
    if str, ok := element.Value.(string); ok {
      temp ,err := strconv.Atoi(str)
      if err != nil {
	fmt.Printf("Atoi for str err\n")
      }
      num = temp
    } else {
      fmt.Printf("Type ereor the value type is \" string \" required\n")
    }
    result += num
  }
  resultStr := strconv.Itoa(result)
  return resultStr
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
// Args[1] is master of worker. Args[2] is the file name.
// Args[3] is sequential or else
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])    
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
