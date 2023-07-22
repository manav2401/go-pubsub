package pubsub

import (
	"testing"
	"time"
)

func TestBrokerBasic(t *testing.T) {
	// create a new broker
	broker := New()

	// create a new topic
	var maxPub uint16 = 3
	var maxSub uint16 = 3
	topic, id, err := broker.NewTopic("test", &maxPub, &maxSub)
	if err != nil {
		t.Errorf("error in creating topic: %s", err)
	}

	// create subscribers
	var subscribers []uint64
	var count uint16 = 3
	for i := 0; i < int(count); i++ {
		id, err := topic.Subscribe()
		if err != nil {
			t.Errorf("error in subscribing to topic: %s", err)
		}
		subscribers = append(subscribers, id)
	}

	// publish message
	msg := Message{Id: id, Msg: []byte("Hello world from producer")}
	t.Log("[", msg.Id, "] Publishing message to topic=", topic.Name, ", msg=", string(msg.Msg))
	topic.Publish(msg)

	time.Sleep(1 * time.Second)

	err = topic.Unsubscribe(subscribers[len(subscribers)-1])
	if err != nil {
		t.Errorf("error in unsubscribing topic: %s", err)
	}

	id2, err := topic.Join()
	if err != nil {
		t.Errorf("error in joining producer: %s", err)
	}
	msg2 := Message{Id: id2, Msg: []byte("Bye from another producer")}
	t.Log("[", msg2.Id, "] Publishing message to topic=", topic.Name, ", msg=", string(msg2.Msg))
	topic.Publish(msg2)

	time.Sleep(1 * time.Second)

	broker.End()
	t.Log("Ending simulation")
}
