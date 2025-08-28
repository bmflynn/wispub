package main

import (
	"gitlab.ssec.wisc.edu/dbrtn/wispub/cmd"
	"log"
)

var version = "<notset>"

func main() {
	if err := cmd.Execute(version); err != nil {
		log.Fatalf("error: %v", err)
	}
}
