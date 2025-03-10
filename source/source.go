// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/conduitio-labs/conduit-connector-kinesis/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/oklog/ulid/v2"
	cmap "github.com/orcaman/concurrent-map/v2"
	"gopkg.in/tomb.v2"
)

type Source struct {
	sdk.UnimplementedSource

	config Config

	// client is the Client for the AWS Kinesis API
	client *kinesis.Client

	// httpClient is the http.Client used for interacting with the kinesis API.
	// We need a custom one so that we can cleanup leaking http connections on
	// the teardown method.
	httpClient *http.Client

	tomb        *tomb.Tomb
	streamMap   cmap.ConcurrentMap[string, *kinesis.SubscribeToShardEventStream]
	buffer      chan opencdc.Record
	consumerARN *string
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

type Shard struct {
	ShardID     *string
	EventStream *kinesis.SubscribeToShardEventStream
}

type kinesisPosition struct {
	SequenceNumber string `json:"sequenceNumber"`
	ShardID        string `json:"shardId"`
}

func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		httpClient: &http.Client{
			Transport: &http.Transport{},
		},
		// If tomb.Wait() is called and no goroutines are started from it, the tomb will cause a deadlock.
		// This can happen if the source connector has Configure() and then Teardown() methods called.
		// We could wrap tomb in another struct and track there in a boolean and a mutex whether
		// a tomb goroutine was started up, but in this narrow case it is simpler to initialize
		// tomb to nil, start it up on Open(), and check whether tomb is nil or not in Teardown()
		// to prevent the deadlock.
		tomb:      nil,
		buffer:    make(chan opencdc.Record, 100),
		streamMap: cmap.New[*kinesis.SubscribeToShardEventStream](),
	})
}

func (s *Source) Open(ctx context.Context, pos opencdc.Position) (err error) {
	s.client, err = common.NewClient(ctx, s.httpClient, s.config.Config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("created source client")
	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	streamOutput, err := s.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: &s.config.StreamName,
	})
	if err != nil {
		return fmt.Errorf("failed to test connection to stream: %w", err)
	}
	sdk.Logger(ctx).Info().Str("streamName", s.config.StreamName).Msg("stream valid")

	consumerResponse, err := s.client.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamOutput.StreamDescription.StreamARN,
		ConsumerName: aws.String("conduit-connector-kinesis-source-" + ulid.Make().String()),
	})
	if err != nil {
		return fmt.Errorf("error registering consumer: %w", err)
	}
	sdk.Logger(ctx).Info().
		Str("consumerName", *consumerResponse.Consumer.ConsumerName).
		Msg("kinesis consumer registered")

	if err := s.waitForConsumer(ctx, consumerResponse.Consumer); err != nil {
		return fmt.Errorf("error waiting for consumer: %w", err)
	}

	s.consumerARN = consumerResponse.Consumer.ConsumerARN
	err = s.subscribeShards(ctx, pos)
	if err != nil {
		return err
	}

	// listenEvents will use tomb.Go, so we initialize it here
	s.tomb = &tomb.Tomb{}
	s.listenEvents(ctx)

	sdk.Logger(ctx).Info().Msg("source ready to be read from")

	return nil
}

func (s *Source) waitForConsumer(ctx context.Context, consumer *types.Consumer) error {
	for count := 1; count <= 5; count++ {
		describedConsumer, err := s.client.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
			ConsumerARN: consumer.ConsumerARN,
		})
		if err != nil {
			return fmt.Errorf("failed to describe stream consumer: %w", err)
		}
		if describedConsumer.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
			return nil
		}

		secsToWait := math.Exp2(float64(count))
		sdk.Logger(ctx).Info().
			Str("consumerARN", *consumer.ConsumerARN).
			Float64("seconds", secsToWait).
			Msg("waiting for consumer to be ready")

		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to wait for consumer to be ready: %w", ctx.Err())
		case <-time.After(time.Duration(secsToWait) * time.Second):
			// retry status check
		}
	}

	return fmt.Errorf("consumer wait timed out")
}

func (s *Source) Read(ctx context.Context) (rec opencdc.Record, err error) {
	select {
	case <-ctx.Done():
		return rec, fmt.Errorf("source read timed out: %w", ctx.Err())
	case rec := <-s.buffer:
		return rec, nil
	}
}

func (s *Source) Ack(_ context.Context, _ opencdc.Position) error {
	// kinesis doesn't have any acking system, so we do nothing here.
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.consumerARN != nil {
		_, err := s.client.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
			ConsumerARN: s.consumerARN,
		})
		if err != nil {
			return fmt.Errorf(
				"error deregistering stream consumer %s: %w",
				*s.consumerARN, err,
			)
		}

		sdk.Logger(ctx).Info().Str("consumerARN", *s.consumerARN).Msg("deregistered stream consumer")
	}

	if s.tomb != nil {
		// tomb was setup, there are goroutines ready to be killed
		s.tomb.Kill(nil)
		if err := s.tomb.Wait(); err != nil {
			return fmt.Errorf("error while waiting listener goroutines to cleanup: %w", err)
		}
		sdk.Logger(ctx).Info().Msg("listener goroutines cleaned up")
	}

	for streamTuple := range s.streamMap.IterBuffered() {
		eventStream := streamTuple.Val
		if err := eventStream.Close(); err != nil {
			sdk.Logger(ctx).Err(err).Msg("error closing stream")
		}
	}
	sdk.Logger(ctx).Info().Msg("closed all streams")

	s.httpClient.CloseIdleConnections()
	sdk.Logger(ctx).Info().Msg("closed httpClient connections")

	return nil
}

func toRecords(kinRecords []types.Record, streamName, shardID string) []opencdc.Record {
	sdkRecs := make([]opencdc.Record, 0, len(kinRecords))

	for _, rec := range kinRecords {
		kinPos := kinesisPosition{
			SequenceNumber: *rec.SequenceNumber,
			ShardID:        shardID,
		}

		kinPosBytes, err := json.Marshal(kinPos)
		if err != nil {
			// should never happen
			panic("failed to marshal position")
		}

		sdkRec := sdk.Util.Source.NewRecordCreate(
			opencdc.Position(kinPosBytes),
			opencdc.Metadata{
				"shardId":        "kinesis-" + shardID,
				"sequenceNumber": "kinesis-" + *rec.SequenceNumber,
			},
			opencdc.RawData(kinPosBytes),
			opencdc.RawData(rec.Data),
		)
		sdkRec.Metadata.SetCollection(streamName)

		sdkRecs = append(sdkRecs, sdkRec)
	}

	return sdkRecs
}

func (s *Source) listenEvents(ctx context.Context) {
	for streamTuple := range s.streamMap.IterBuffered() {
		shardID, eventStream := streamTuple.Key, streamTuple.Val

		s.tomb.Go(func() error {
			for {
				select {
				case event := <-eventStream.Events():
					if event == nil {
						err := s.resubscribeShard(ctx, shardID)
						if err != nil {
							return fmt.Errorf("error resubscribing to shard %s: %w", shardID, err)
						}
					}

					subsEvent, ok := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
					if !ok {
						return fmt.Errorf("invalid received event, got %v", event)
					}

					eventValue := subsEvent.Value

					if len(eventValue.Records) > 0 {
						recs := toRecords(eventValue.Records, s.config.StreamName, shardID)

						for _, record := range recs {
							s.buffer <- record
						}
						sdk.Logger(ctx).Trace().Msg("sent all records")
					}
				case <-s.tomb.Dying():
					sdk.Logger(ctx).Debug().Msg("tomb kill called, exiting listener goroutine")
					return nil
				case <-ctx.Done():
					sdk.Logger(ctx).Debug().Msg("context done, exiting listener goroutine")
					return nil
				// refresh the subscription after 5 minutes since that is when kinesis subscriptions go stale
				case <-time.After(time.Minute*4 + time.Second*55):
					for streamTuple := range s.streamMap.IterBuffered() {
						stream := streamTuple.Val
						if stream != nil {
							stream.Close()
						}

						err := s.resubscribeShard(ctx, shardID)
						if err != nil {
							return fmt.Errorf("error resubscribing to shard %s: %w", shardID, err)
						}
					}
				}
			}
		})
	}
}

func (s *Source) subscribeShards(ctx context.Context, position opencdc.Position) error {
	var startingPosition types.StartingPosition
	switch {
	case position != nil:
		pos, err := parsePosition(position)
		if err != nil {
			return err
		}

		startingPosition.Type = types.ShardIteratorTypeAfterSequenceNumber
		startingPosition.SequenceNumber = &pos.SequenceNumber
	case s.config.StartFromLatest:
		startingPosition.Type = types.ShardIteratorTypeLatest
	case !s.config.StartFromLatest:
		startingPosition.Type = types.ShardIteratorTypeTrimHorizon
	}

	logEvt := sdk.Logger(ctx).Info().Str("type", string(startingPosition.Type))
	if seqNum := startingPosition.SequenceNumber; seqNum != nil {
		logEvt = logEvt.Str("sequenceNumber", *seqNum)
	}
	logEvt.Msg("starting position")

	listShardsResponse, err := s.client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: &s.config.StreamName,
	})
	if err != nil {
		return fmt.Errorf("error retrieving kinesis shards: %w", err)
	}

	// get iterators for shards
	for _, shard := range listShardsResponse.Shards {
		subscriptionResponse, err := s.client.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN:      s.consumerARN,
			ShardId:          shard.ShardId,
			StartingPosition: &startingPosition,
		})
		if err != nil {
			return fmt.Errorf("error creating stream subscription: %w", err)
		}

		s.streamMap.Set(*shard.ShardId, subscriptionResponse.GetStream())
		sdk.Logger(ctx).Info().Str("shardID", *shard.ShardId).Msg("subscribed to shard")
	}

	return nil
}

func (s *Source) resubscribeShard(ctx context.Context, shardID string) error {
	subscriptionResponse, err := s.client.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
		ConsumerARN: s.consumerARN,
		ShardId:     aws.String(shardID),
		StartingPosition: &types.StartingPosition{
			Type: types.ShardIteratorTypeLatest,
		},
	})
	if err != nil {
		return fmt.Errorf("error creating stream subscription: %w", err)
	}

	s.streamMap.Set(shardID, subscriptionResponse.GetStream())
	sdk.Logger(ctx).Info().Str("shardID", shardID).Msg("resubscribed to shard")
	return nil
}

func parsePosition(pos opencdc.Position) (kinesisPosition, error) {
	var kinPos kinesisPosition
	err := json.Unmarshal(pos, &kinPos)
	if err != nil {
		return kinPos, fmt.Errorf("failed to parse sdk position: %w", err)
	}

	return kinPos, nil
}
