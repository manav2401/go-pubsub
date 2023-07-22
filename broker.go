package pubsub

import (
	"errors"
	"log"
	"math/rand"
	"sync"
)

// defaults
var (
	// Maximum topics handled by a broker
	MaxTopics uint64 = 10

	// Maximum publishers handled by a topic
	MaxPublishers uint64 = 10

	// Maximum subscribers handled by a topic
	MaxSubscribers uint64 = 10

	// Maximum topic channel buffer
	MaxChannelLength uint64 = 100
)

var (
	// publishers is a mapping of topic name to a publisher.
	publishers map[string]map[uint64]*Publisher

	// subscribers is a mapping of topic name to a list of subscriber.
	subscribers map[string]map[uint64]*Subscriber
)

var (
	errInvalidTopcicName  = errors.New("topic name is invalid")
	errDupliacteTopicName = errors.New("topic name is already taken")
	errPublisherOverflow  = errors.New("max publisher limit reached")
	errSubscriberOverflow = errors.New("max subscriber limit reached")
	errInvalidId          = errors.New("invalid id")
)

type Broker struct {
	lock   sync.Mutex        // lock
	topics map[string]*Topic // list of topics
}

type Topic struct {
	lock           sync.Mutex // topic level lock
	name           string     // Name of the topic
	maxPublishers  uint64     // MaxPublishers allowed in the topic
	maxSubscribers uint64     // MaxSubscribers allowed in the topic
}

type Publisher struct {
	id      uint64             // publisher id
	pubChan chan PubSubMessage // publisher channel
	done    chan bool          // End channel
}

type Subscriber struct {
	id      uint64             // subscriber id
	subChan chan PubSubMessage // subscriber channel
	done    chan bool          // End channel
}

type PubSubMessage struct {
	Id  uint64 // id of the sender
	Msg []byte // message itself
}

// New creates a new Broker.
func New() *Broker {
	publishers = make(map[string]map[uint64]*Publisher)
	subscribers = make(map[string]map[uint64]*Subscriber)
	return &Broker{topics: make(map[string]*Topic)}
}

// GetTopic by name
func (b *Broker) Topic(name string) *Topic {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.topics[name]
}

// NewTopic creates a new topic given the name and max possible
// publishers and subscribers. Sets to default if 0 is passed.
func (b *Broker) NewTopic(name string, maxPub uint64, maxSub uint64) (*Topic, uint64, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if name == "" {
		return nil, 0, errInvalidTopcicName
	}

	if _, ok := b.topics[name]; ok {
		return nil, 0, errDupliacteTopicName
	}

	if maxPub == 0 {
		maxPub = MaxPublishers
	}

	if maxSub == 0 {
		maxSub = MaxSubscribers
	}

	// Create a new topic
	topic := &Topic{
		name:           name,
		maxPublishers:  maxPub,
		maxSubscribers: maxSub,
	}

	publishers[name] = make(map[uint64]*Publisher)
	subscribers[name] = make(map[uint64]*Subscriber)

	// join the topic direcly for the creator
	id, err := topic.Join()
	if err != nil {
		return nil, 0, err
	}

	return topic, id, nil
}

// Join a specific topic as a publisher. Returns an ID back.
func (t *Topic) Join() (uint64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// check if we can accomodate a new publisher
	if len(publishers[t.name]) == int(t.maxPublishers) {
		return 0, errPublisherOverflow
	}

	var id uint64
	// create a non-conflicting random id
	for {
		id = uint64(rand.Int63())
		if id == 0 {
			continue
		}
		if _, ok := publishers[t.name][id]; ok {
			continue
		}
		break
	}

	// set the publisher
	publishers[t.name][id] = &Publisher{
		id:      id,
		pubChan: make(chan PubSubMessage, MaxChannelLength),
		done:    make(chan bool, 1),
	}

	// listen for messages
	go t.listenPub(id)

	return id, nil
}

// Leave a specific topic as a publisher
func (t *Topic) Leave(id uint64) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// check publisher validity
	pub, ok := publishers[t.name][id]
	if !ok {
		return errInvalidId
	}

	// send signal to end go listening routine
	pub.done <- true

	// close and clear publisher
	pub.Close()
	delete(publishers[t.name], id)

	return nil
}

// Publish publishes a message to a topic.
func (t *Topic) Publish(msg PubSubMessage) error {
	// check if message is sent by publisher or not
	if _, ok := publishers[t.name][msg.Id]; !ok {
		return errInvalidId
	}

	// send message to corresponding publisher channel
	publishers[t.name][msg.Id].pubChan <- msg

	return nil
}

// Subscribe to a topic for receiving messages.
func (t *Topic) Subscribe() (uint64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// check if we can accomodate a new subscriber
	if len(subscribers[t.name]) == int(t.maxSubscribers) {
		return 0, errSubscriberOverflow
	}

	var id uint64
	// create a non-conflicting random id
	for {
		id = uint64(rand.Int63())
		if id == 0 {
			continue
		}
		if _, ok := subscribers[t.name][id]; ok {
			continue
		}
		break
	}

	// add a new subscriber
	subscribers[t.name][id] = &Subscriber{
		id:      id,
		subChan: make(chan PubSubMessage, MaxChannelLength),
		done:    make(chan bool, 1),
	}

	// listen for incoming messages
	go t.listenSub(id)

	return id, nil
}

// Unsubscribe to a topic for receiving messages.
func (t *Topic) Unsubscribe(id uint64) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// check subscriber validity
	sub, ok := subscribers[t.name][id]
	if !ok || id != sub.id {
		return errInvalidId
	}

	// send signal to end go listening routine
	sub.done <- true

	// close and clear subscriber
	sub.Close()

	// remove from subscribers
	delete(subscribers[t.name], id)

	return nil
}

// listenPub listens to incoming messages (from publishers)
func (t *Topic) listenPub(id uint64) {
	for {
		select {
		case msg := <-publishers[t.name][id].pubChan:
			// send message to subscribers
			for _, v := range subscribers[t.name] {
				v.subChan <- msg
			}
		case <-publishers[t.name][id].done:
			return
		}
	}
}

// listenSub listens to messages for subscribers
func (t *Topic) listenSub(id uint64) {
	for {
		select {
		case msg := <-subscribers[t.name][id].subChan:
			log.Print("[", id, "] Message received from=", msg.Id, " in topic=", t.name, ", msg=", string(msg.Msg))
		case <-subscribers[t.name][id].done:
			return
		}
	}
}

// Close the publisher
func (p *Publisher) Close() {
	close(p.pubChan)
	close(p.done)
}

// Close the subscriber
func (s *Subscriber) Close() {
	close(s.subChan)
	close(s.done)
}

// Close the topic of a broker.
func (b *Broker) Close(topic *Topic) {
	b.lock.Lock()
	defer b.lock.Unlock()

	// remove from publishers
	if _, ok := publishers[topic.name]; ok {
		for _, pub := range publishers[topic.name] {
			pub.Close()
		}
		delete(publishers, topic.name)
	}

	// remove from subscribers
	if _, ok := subscribers[topic.name]; ok {
		for _, sub := range subscribers[topic.name] {
			sub.Close()
		}
		delete(subscribers, topic.name)
	}

	// remove from broker
	delete(b.topics, topic.name)
}

// End the broker service.
func (b *Broker) End() {
	for _, topic := range b.topics {
		b.Close(topic)
	}
	log.Print("Graceful Shutdown!")
}
