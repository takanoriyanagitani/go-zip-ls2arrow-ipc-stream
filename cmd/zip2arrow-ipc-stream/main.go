package main

import (
	"log"
	"os"

	zi "github.com/takanoriyanagitani/go-zip-ls2arrow-ipc-stream"
)

var zipFilename zi.ZipFilename = zi.ZipFilename(
	os.Getenv("ENV_ZIP_FILE_NAME"),
)

func main() {
	if "" == zipFilename {
		log.Printf("ENV_ZIP_FILE_NAME undefined.\n")
		os.Exit(1)
	}

	err := zipFilename.ToStdout()
	if nil != err {
		log.Fatalf("%v\n", err)
	}
}
