package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

const esEndpoint = "http://your-elasticsearch-endpoint:9200/sflow/data"

type SFlowData struct {
	Timestamp string `json:"timestamp"`
	Data      []byte `json:"data"`
}

func main() {
	http.HandleFunc("/sflow", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		data := SFlowData{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Data:      []byte{},
		}

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data.Data = b

		err = sendDataToES(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "sFlow data received and stored in Elasticsearch")
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func sendDataToES(data SFlowData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := http.Post(esEndpoint, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected response from Elasticsearch: %s %s", resp.Status, string(body))
	}

	return nil
}

