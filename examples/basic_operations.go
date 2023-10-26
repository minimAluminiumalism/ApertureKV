package main

import (
	"fmt"

	aperture "github.com/minimAluminiumalism/ApertureKV"
)

func main()  {
	opts := aperture.DefaultOptions
	opts.DirPath = "/tmp/aperturekv"
	db, err := aperture.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val =", string(val))
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}