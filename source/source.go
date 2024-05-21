package source

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
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

	httpClient *http.Client

	tomb        *tomb.Tomb
	streamMap   cmap.ConcurrentMap[string, *kinesis.SubscribeToShardEventStream]
	buffer      chan sdk.Record
	consumerARN *string
}

type Shard struct {
	ShardID     *string
	EventStream *kinesis.SubscribeToShardEventStream
}

type kinesisPosition struct {
	SequenceNumber string
	ShardID        string
}

func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		httpClient: &http.Client{
			Transport: &http.Transport{},
		},
		tomb:      &tomb.Tomb{},
		buffer:    make(chan sdk.Record, 100),
		streamMap: cmap.New[*kinesis.SubscribeToShardEventStream](),
	}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Configure the creds for the client
	var cfgOptions []func(*config.LoadOptions) error

	cfgOptions = append(cfgOptions, config.WithHTTPClient(s.httpClient))

	cfgOptions = append(cfgOptions, config.WithRegion(s.config.AWSRegion))
	cfgOptions = append(cfgOptions, config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			s.config.AWSAccessKeyID,
			s.config.AWSSecretAccessKey,
			"")))

	if s.config.AWSURL != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               s.config.AWSURL,
				SigningRegion:     s.config.AWSRegion,
				HostnameImmutable: true,
			}, nil
		})
		withResolverOpt := config.WithEndpointResolverWithOptions(resolver)

		cfgOptions = append(cfgOptions, withResolverOpt)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, cfgOptions...)
	if err != nil {
		return fmt.Errorf("failed to load aws config with given credentials : %w", err)
	}

	s.client = kinesis.NewFromConfig(awsCfg)

	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	_, err := s.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &s.config.StreamARN,
	})
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("error when attempting to test connection to stream")
		return err
	}
	sdk.Logger(ctx).Info().Str("streamARN", s.config.StreamARN).Msg("stream valid")

	consumerResponse, err := s.client.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    &s.config.StreamARN,
		ConsumerName: aws.String("conduit-connector-kinesis-source-" + ulid.Make().String()),
	})
	if err != nil {
		return fmt.Errorf("error registering consumer: %w", err)
	}
	sdk.Logger(ctx).Info().
		Str("consumerName", *consumerResponse.Consumer.ConsumerName).
		Msg("kinesis consumer registered")

	s.waitForConsumer(ctx, consumerResponse.Consumer)

	s.consumerARN = consumerResponse.Consumer.ConsumerARN
	err = s.subscribeShards(ctx, pos)
	if err != nil {
		return err
	}

	go s.listenEvents(ctx)

	return nil
}

func (s *Source) waitForConsumer(ctx context.Context, consumer *types.Consumer) error {
	for count := 1; count <= 5; count++ {
		secsToWait := math.Exp2(float64(count))
		sdk.Logger(ctx).Info().
			Str("consumerARN", *consumer.ConsumerARN).
			Float64("seconds", secsToWait).
			Msg("waiting for consumer to be ready")

		time.Sleep(time.Duration(secsToWait) * time.Second)

		describedConsumer, err := s.client.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
			ConsumerARN:  consumer.ConsumerARN,
			ConsumerName: consumer.ConsumerName,
			StreamARN:    &s.config.StreamARN,
		})
		if err != nil {
			return err
		}
		if describedConsumer.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
			return nil
		}
	}

	return fmt.Errorf("consumer wait timed out")
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	case rec := <-s.buffer:
		return rec, nil
	}
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	pos, err := parsePosition(position)
	if err != nil {
		return err
	}

	sdk.Logger(ctx).Debug().Msg(fmt.Sprintf("ack'd record with shardID: %s and sequence number: %s", pos.ShardID, pos.SequenceNumber))
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.consumerARN != nil {
		_, err := s.client.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
			ConsumerARN: s.consumerARN,
			StreamARN:   &s.config.StreamARN,
		})
		if err != nil {
			return fmt.Errorf(
				"error deregistering stream consumer %s: %w",
				*s.consumerARN, err,
			)
		}

		sdk.Logger(ctx).Info().Str("consumerARN", *s.consumerARN).Msg("deregistered stream consumer")
	}

	s.tomb.Kill(nil)
	if err := s.tomb.Wait(); err != nil {
		return fmt.Errorf("error while waiting listener goroutines to cleanup: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("listener goroutines cleaned up")

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

func toRecords(kinRecords []types.Record, shardID string) []sdk.Record {
	var sdkRecs []sdk.Record

	for _, rec := range kinRecords {
		kinPos := kinesisPosition{
			SequenceNumber: *rec.SequenceNumber,
			ShardID:        shardID,
		}

		kinPosBytes, _ := json.Marshal(kinPos)
		sdkRec := sdk.Util.Source.NewRecordCreate(
			sdk.Position(kinPosBytes),
			sdk.Metadata{
				"shardId":        "kinesis-" + shardID,
				"sequenceNumber": "kinesis-" + *rec.SequenceNumber,
			},
			sdk.RawData(kinPosBytes),
			sdk.RawData(rec.Data),
		)

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
							return fmt.Errorf("error resubscribing to shard: %w", err)
						}
					}

					eventValue := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent).Value

					if len(eventValue.Records) > 0 {
						recs := toRecords(eventValue.Records, shardID)

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
							return fmt.Errorf("error resubscribing to shard: %w", err)
						}
					}
				}
			}
		})
	}
}

func (s *Source) subscribeShards(ctx context.Context, position sdk.Position) error {
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
		StreamARN: &s.config.StreamARN,
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

func parsePosition(pos sdk.Position) (kinesisPosition, error) {
	var kinPos kinesisPosition
	err := json.Unmarshal(pos, &kinPos)
	if err != nil {
		return kinPos, err
	}

	return kinPos, nil
}
