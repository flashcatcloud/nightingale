package rpc

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/didi/nightingale/v4/src/common/dataobj"
	"github.com/didi/nightingale/v4/src/common/stats"
	"github.com/didi/nightingale/v4/src/modules/server/aggr"
	"github.com/didi/nightingale/v4/src/modules/server/cache"

	"github.com/segmentio/kafka-go"
	"github.com/toolkits/pkg/concurrent/semaphore"
	"github.com/toolkits/pkg/logger"
)

var Reader *kafka.Reader

func consumer() {
	if !aggr.AggrConfig.Enabled {
		return
	}

	if aggr.AggrConfig.ConsumerGroup == "" {
		aggr.AggrConfig.ConsumerGroup = "aggr_consumer"
	}

	Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     aggr.AggrConfig.KafkaAddrs,
		GroupID:     aggr.AggrConfig.ConsumerGroup,
		Topic:       aggr.AggrConfig.KafkaAggrOutTopic,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,    // 1
		MaxBytes:    10e6, // 10MB
	})

	CosumeMsg(context.Background())
}

func CosumeMsg(ctx context.Context) {
	sema := semaphore.NewSemaphore(5)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			sema.Acquire()
			go porcessMsg(ctx, sema)
		}
	}
}

func porcessMsg(ctx context.Context, sema *semaphore.Semaphore) {
	defer sema.Release()

	m, err := Reader.ReadMessage(ctx)
	if err != nil {
		if err.Error() == "context canceled" {
			return
		}

		logger.Warning("failed to fetch message:", err)
		return
	}

	msgs := &dataobj.AggrOut{}
	if err := json.Unmarshal(m.Value, msgs); err != nil {
		logger.Errorf("decode message:%s failed, err:%v", string(m.Value), err)
		return
	}

	item := aggrOut2MetricValue(msgs)
	var args []*dataobj.MetricValue
	args = append(args, item)
	stats.Counter.Set("aggr.points.in", len(args))
	PushData(args)
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
