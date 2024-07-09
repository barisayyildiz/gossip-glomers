package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n}

	n.Handle("topology", s.topology)
	n.Handle("broadcast", s.broadcast)
	n.Handle("read", s.read)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	n     *maelstrom.Node
	ids   []int
	mutex sync.Mutex
}

func (s *server) read(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	response := map[string]any{
		"type":     "read_ok",
		"messages": s.ids,
	}
	return s.n.Reply(msg, response)
}

func (s *server) broadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mutex.Lock()

	id := int(body["message"].(float64))
	s.ids = append(s.ids, id)

	s.mutex.Unlock()

	response := map[string]any{
		"type": "broadcast_ok",
	}

	return s.n.Reply(msg, response)
}

func (s *server) topology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	response := map[string]any{
		"type": "topology_ok",
	}

	return s.n.Reply(msg, response)
}
