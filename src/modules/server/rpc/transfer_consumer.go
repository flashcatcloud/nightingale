package rpc

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/didi/nightingale/v4/src/common/dataobj"
	"github.com/didi/nightingale/v4/src/common/stats"
	"github.com/didi/nightingale/v4/src/modules/server/aggr"
	"github.com/didi/nightingale/v4/src/modules/server/cache"

	"github.com/Shopify/sarama"
	"github.com/toolkits/pkg/logger"
)

func consumer() {
	if !aggr.AggrConfig.Enabled {
		return
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = aggr.AggrConfig.ConsumerError

	if !config.Consumer.Return.Errors {
		fw, err := os.OpenFile("kafka_error.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("create consumer err log err:%v", err)
		}
		sarama.Logger = log.New(fw, "[Sarama] ", log.LstdFlags)
	}

	// Specify brokers address. This is default one
	brokers := aggr.AggrConfig.KafkaAddrs

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("create consumer err:%v", err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			logger.Error(err)
		}
	}()

	topic := aggr.AggrConfig.KafkaAggrOutTopic
	// How to decide partition, is it fixed value...?
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("create consumer err:%v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				if err != nil {
					logger.Error("consumer ", err)
				}
			case msg := <-consumer.Messages():
				if msg == nil {
					continue
				}

				msgCount++

				out := &dataobj.AggrOut{}
				err := json.Unmarshal(msg.Value, out)
				if err != nil {
					logger.Error(err)
				} else {
					item := aggrOut2MetricValue(out)
					var args []*dataobj.MetricValue
					args = append(args, item)

					stats.Counter.Set("aggr.points.in", len(args))
					PushData(args)
				}

			case <-signals:
				logger.Error("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	logger.Error("Processed", msgCount, "messages")
}

func aggrOut2MetricValue(out *dataobj.AggrOut) *dataobj.MetricValue {
	return &dataobj.MetricValue{
		Nid:          out.Data.Nid,
		Metric:       cache.AggrCalcMap.GetMetric(out.Data.Sid),
		Timestamp:    out.Data.Timestamp / 1000,
		Step:         out.Data.Step,
		Tags:         strings.Replace(out.Data.GroupTag, "||", ",", -1),
		ValueUntyped: out.Data.Value,
		CounterType:  "GAUGE",
	}
}
