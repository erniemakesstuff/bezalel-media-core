package main

import (
	"fmt"
	"log"
	"net/http"

	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

func main() {
	http.HandleFunc("/", handler)
	dynamo_configuration.Init()
	log.Fatal(http.ListenAndServe(":8080", nil))
}
