# Go pubsub
A simple pub sub mechanism in Go

It uses a simple broker based mechanism to manage different topics of communication. 

### Usage

Here's a sample code to use this go-pubsub. 

```go
// Initialise a new broker
broker := pubsub.NewBroker()

// Create a new topic (and also get auto-subscribed)
var maxSub uint16 = 3
topic := broker.NewTopic("topic-name", &maxSub)

// Publish new messages to topic
topic.Publish([]("Hello World!"))

// Get a new topic and subscribe to it 
t := broker.Topic("topic-name")
id, err := t.Subscribe()

// Unsubscribe using subscriber id
err := topic.Unsubscribe(id)

// Close the broker
broker.End()
```
