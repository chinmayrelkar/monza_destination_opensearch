package monza_destination_opensearch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chinmayrelkar/monza"
	"github.com/google/uuid"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
	"github.com/sirupsen/logrus"
)

func DefaultOpenSearchConfig() Config {
	return Config{
		Config: opensearch.Config{
			Addresses:     []string{"http://localhost:5432"},
			RetryOnStatus: []int{502, 503, 504, 429},
			RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
			MaxRetries:    5,
		},
		NumWorkers: 4,
		FlushInterval: time.Second,
		DefaultIndex: "monza",
	}
}

type Config struct {
	opensearch.Config
	NumWorkers    int
	FlushInterval time.Duration
	DefaultIndex  string
}

func Get(ctx context.Context, config Config) monza.Destination {
	return &client{
		config: config,
	}
}

type client struct {
	config       Config
	client       *opensearch.Client
	eventChannel chan *monza.Event
}

// Setup implements monza.Destination.
func (c *client) Setup(ctx context.Context) error {
	client, err := opensearch.NewClient(c.config.Config)
	if err != nil {
		return err
	}
	_, err = opensearchapi.PingRequest{}.Do(ctx, client)
	if err != nil {
		return err
	}
	c.client = client
	return c.listen(ctx)
}

// Record implements monza.Destination.
func (c *client) Record(ctx context.Context, event monza.Event) {
	c.eventChannel <- &event
}

// Teardown implements destinations.Destination.
func (c *client) Teardown(ctx context.Context) {
	c.eventChannel <- nil
}

func (c *client) listen(ctx context.Context) error {
	indexer, err := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client:        c.client,
		Index:         c.config.DefaultIndex,
		NumWorkers:    c.config.NumWorkers,
		FlushInterval: c.config.FlushInterval,
	})
	if err != nil {
		return err
	}

	if indexer == nil {
		return errors.New("destinations.opensearch: Failed to create Indexer")
	}

	eventChannel := make(chan *monza.Event)
	go func() {
		for {
			e := <-eventChannel
			if e == nil {
				// go routine exit clause
				break
			}

			if e.ID == 0 {
				random, _ := uuid.NewRandom()
				e.ID = int64(random.ID()) + time.Now().Unix()
			}
			err := indexer.Add(ctx, opensearchutil.BulkIndexerItem{
				Index:      string(e.ServiceID) + "-monza-" + time.Now().Format("2006-01-02"),
				Action:     "index",
				DocumentID: toOpenSearchDocumentID(*e),
				Body:       bytes.NewReader(e.JSON()),
			})
			if err != nil {
				logrus.Error(err)
			}
		}
	}()
	c.eventChannel = eventChannel
	return nil
}

func toOpenSearchDocumentID(e monza.Event) string {
	return fmt.Sprintf("doc-%d", e.ID)
}
