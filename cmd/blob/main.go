package main

import (
	"flag"
	"fmt"
	"github.com/gonutz/blob"
	"io/ioutil"
	"os"
	"path/filepath"
)

var (
	folder  = flag.String("folder", "", "File or folder to be blobbed")
	outPath = flag.String("out", "", "Output path")
)

func main() {
	flag.Parse()

	if *folder == "" {
		fail("folder not specified")
	}
	if *outPath == "" {
		fail("output path not specified")
	}

	f, err := os.Lstat(*folder)
	if err != nil {
		fail("cannot find folder " + *folder + ": " + err.Error())
	}

	if !f.IsDir() {
		fail(*folder + " is not a folder")
		return
	}

	files, err := ioutil.ReadDir(*folder)
	if err != nil {
		fail("cannot read folder: " + err.Error())
	}

	var b blob.Blob
	for _, f := range files {
		if !f.IsDir() {
			data, err := ioutil.ReadFile(filepath.Join(*folder, f.Name()))
			if err == nil {
				b.Append(f.Name(), data)
			}
		}
	}

	outFile, err := os.Create(*outPath)
	if err != nil {
		fail("cannot create output file: " + err.Error())
	}
	defer outFile.Close()
	b.Write(outFile)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func fail(msg string) {
	fmt.Println("error:", msg)
	flag.Usage()
	os.Exit(1)
}
