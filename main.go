package main

import (
	"cl-canal/canal"
	"fmt"
)

func main() {

	err := canal.BinLogListener()
	if err != nil {
		fmt.Errorf(fmt.Sprintf("BinLogListener ERROR: %v", err))
	}

}
