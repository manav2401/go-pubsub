# Go pubsub
A simple pub sub mechanism in Go

It uses a simple broker based mechanism to manage different topics of communication. 

### Usage

Here's a sample code to use this go-pubsub. 

```go
// Initialise a new broker
broker := pubsub.NewBroker()

// Create a new topic (and also get auto-subscribed)
topic := broker.NewTopic("topic-name", 10)

// Publish new messages to topic
topic.Publish([]("Hello World!"))

// Get a new topic and subscribe to it. Returns the id and channel
// to listen to published messages.  
t := broker.Topic("topic-name")
id, ch, err := t.Subscribe()

// Unsubscribe using subscriber id
err := topic.Unsubscribe(id)

// Close the broker
broker.End()
```
