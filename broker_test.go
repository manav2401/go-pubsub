package pubsub

import (
	"testing"
	"time"
)

func TestBrokerBasic(t *testing.T) {
	// create a new broker
	broker := New()

	// create a new topic
	topic, id, err := broker.NewTopic("test", 10, 10)
	if err != nil {
		t.Errorf("error in creating topic: %s", err)
	}

	// create subscribers
	subscribers := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		id, ch, err := topic.Subscribe()
		if err != nil {
			t.Errorf("error in subscribing to topic: %s", err)
		}

		subscribers[i] = id

		// Keep listening in bg
		go func(i int, ch chan PubSubMessage) {
			for {
				select {
				case msg := <-ch:
					t.Logf("[%d]: Message received from=%d in topic=%s - msg: %s\n", id, msg.Id, topic.name, string(msg.Msg))
					return
				case <-time.After(2 * time.Second):
					t.Logf("[%d]: Timeout, index=%d\n", id, i)
					return
				}
			}
		}(i, ch)
	}

	// publish message
	msg := PubSubMessage{Id: id, Msg: []byte("Hello world from producer")}
	t.Logf("[%d]: Publishing message to topic=%s - msg: %s\n", msg.Id, topic.name, string(msg.Msg))
	topic.Publish(msg)

	id2, err := topic.Join()
	if err != nil {
		t.Errorf("error in joining producer: %s", err)
	}
	msg2 := PubSubMessage{Id: id2, Msg: []byte("Bye from another producer")}
	t.Logf("[%d]: Publishing message to topic=%s - msg: %s\n", msg2.Id, topic.name, string(msg2.Msg))
	topic.Publish(msg2)

	// Unsubscribe all
	for i := 0; i < len(subscribers); i++ {
		err := topic.Unsubscribe(subscribers[i])
		if err != nil {
			t.Errorf("error in unsubscribing topic: %s", err)
		}
	}

	broker.End()
}
