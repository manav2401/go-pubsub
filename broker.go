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
	MaxTopics uint16 = 10

	// Maximum publishers handled by a topic
	MaxPublishers uint16 = 10

	// Maximum subscribers handled by a topic
	MaxSubscribers uint16 = 10

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
	lock           sync.Mutex // lock
	Name           string     // Name of the topic
	MaxPublishers  uint16     // MaxPublishers allowed in the topic
	MaxSubscribers uint16     // MaxSubscribers allowed in the topic
}

type Publisher struct {
	id      uint64       // publisher id
	pubChan chan Message // publisher channel
	done    chan bool    // End channel
}

type Subscriber struct {
	id      uint64       // subscriber id
	subChan chan Message // subscriber channel
	done    chan bool    // End channel
}

type Message struct {
	Id  uint64 // id of the sender
	Msg []byte // message itself
}

// New creates a new Broker.
func New() *Broker {
	publishers = make(map[string]map[uint64]*Publisher)
	subscribers = make(map[string]map[uint64]*Subscriber)
	b := &Broker{topics: make(map[string]*Topic)}
	// msg := Message{id: uint64(1), msg: []byte("Hello")}
	return b
}

// GetTopic by name
func (b *Broker) Topic(name string) *Topic {
	b.lock.Lock()
	defer b.lock.Unlock()

	if topic, ok := b.topics[name]; ok {
		return topic
	}
	return nil
}

// NewTopic creates a new Topic given the topic name and max subscribers
func (b *Broker) NewTopic(name string, maxPub *uint16, maxSub *uint16) (*Topic, uint64, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if name == "" {
		return nil, 0, errInvalidTopcicName
	}

	if _, ok := b.topics[name]; ok {
		return nil, 0, errDupliacteTopicName
	}

	if maxPub == nil {
		maxPub = &MaxPublishers
	}
	if maxSub == nil {
		maxSub = &MaxSubscribers
	}

	// Create a new topic
	topic := &Topic{
		Name:           name,
		MaxPublishers:  *maxPub,
		MaxSubscribers: *maxSub,
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

// Join a specific topic as a publisher
func (t *Topic) Join() (uint64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// check if we can accomodate a new publisher
	if len(publishers[t.Name]) == int(t.MaxPublishers) {
		return 0, errPublisherOverflow
	}

	var id uint64
	// create a non-conflicting random id
	for {
		id = uint64(rand.Int63())
		if id == 0 {
			continue
		}
		if _, ok := publishers[t.Name][id]; ok {
			continue
		}
		break
	}

	// set the publisher
	publishers[t.Name][id] = &Publisher{
		id:      id,
		pubChan: make(chan Message, MaxChannelLength),
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
	pub, ok := publishers[t.Name][id]
	if !ok || id != pub.id {
		return errInvalidId
	}

	// send signal to end go listening routine
	pub.done <- true

	// close and clear publisher
	pub.Close()
	delete(publishers[t.Name], id)

	return nil
}

// Publish publishes a message to a topic.
func (t *Topic) Publish(msg Message) error {
	// check if message is sent by publisher or not
	if _, ok := publishers[t.Name][msg.Id]; !ok {
		return errInvalidId
	}

	// send message to corresponding publisher channel
	publishers[t.Name][msg.Id].pubChan <- msg

	return nil
}

// Subscribe to a topic for receiving messages.
func (t *Topic) Subscribe() (uint64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// check if we can accomodate a new subscriber
	if len(subscribers[t.Name]) == int(t.MaxSubscribers) {
		return 0, errSubscriberOverflow
	}

	var id uint64
	// create a non-conflicting random id
	for {
		id = uint64(rand.Int63())
		if id == 0 {
			continue
		}
		if _, ok := subscribers[t.Name][id]; ok {
			continue
		}
		break
	}

	// add a new subscriber
	subscribers[t.Name][id] = &Subscriber{
		id:      id,
		subChan: make(chan Message, MaxChannelLength),
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
	sub, ok := subscribers[t.Name][id]
	if !ok || id != sub.id {
		return errInvalidId
	}

	// send signal to end go listening routine
	sub.done <- true

	// close and clear subscriber
	sub.Close()

	// remove from subscribers
	delete(subscribers[t.Name], id)

	return nil
}

// listenPub listens to incoming messages (from publishers)
func (t *Topic) listenPub(id uint64) {
	for {
		select {
		case msg := <-publishers[t.Name][id].pubChan:
			// send message to subscribers
			for _, v := range subscribers[t.Name] {
				v.subChan <- msg
			}
		case <-publishers[t.Name][id].done:
			return
		}
	}
}

// listenSub listens to messages for subscribers
func (t *Topic) listenSub(id uint64) {
	for {
		select {
		case msg := <-subscribers[t.Name][id].subChan:
			log.Print("[", id, "] Message received from=", msg.Id, " in topic=", t.Name, ", msg=", string(msg.Msg))
		case <-subscribers[t.Name][id].done:
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
	if _, ok := publishers[topic.Name]; ok {
		for _, pub := range publishers[topic.Name] {
			pub.Close()
		}
		delete(publishers, topic.Name)
	}

	// remove from subscribers
	if _, ok := subscribers[topic.Name]; ok {
		for _, sub := range subscribers[topic.Name] {
			sub.Close()
		}
		delete(subscribers, topic.Name)
	}

	// remove from broker
	delete(b.topics, topic.Name)
}

// End the broker service.
func (b *Broker) End() {
	for _, topic := range b.topics {
		b.Close(topic)
	}
	log.Print("Graceful Shutdown!")
}
