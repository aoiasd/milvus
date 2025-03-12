package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type upsertTaskByStreamingService struct {
	*upsertTask
	ezID string
}

func (ut *upsertTaskByStreamingService) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-Execute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.String("collectionName", ut.req.CollectionName))

	if ezID, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.CripherEZIDKey, ut.schema.Properties); ok {
		ut.ezID = ezID
	}

	insertMsgs, err := ut.packInsertMessage(ctx)
	if err != nil {
		log.Warn("pack insert message failed", zap.Error(err))
		return err
	}
	deleteMsgs, err := ut.packDeleteMessage(ctx)
	if err != nil {
		log.Warn("pack delete message failed", zap.Error(err))
		return err
	}

	messages := append(insertMsgs, deleteMsgs...)
	resp := streaming.WAL().AppendMessages(ctx, messages...)
	if err := resp.UnwrapFirstError(); err != nil {
		log.Warn("append messages to wal failed", zap.Error(err))
		return err
	}
	// Update result.Timestamp for session consistency.
	ut.result.Timestamp = resp.MaxTimeTick()
	return nil
}

func (ut *upsertTaskByStreamingService) packInsertMessage(ctx context.Context) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy insertExecute upsert %d", ut.ID()))
	defer tr.Elapse("insert execute done when insertExecute")

	collectionName := ut.upsertMsg.InsertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, ut.req.GetDbName(), collectionName)
	if err != nil {
		return nil, err
	}
	ut.upsertMsg.InsertMsg.CollectionID = collID
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collID))
	getCacheDur := tr.RecordSpan()

	getMsgStreamDur := tr.RecordSpan()
	channelNames, err := ut.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed when insertExecute",
			zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}

	log.Debug("send insert request to virtual channels when insertExecute",
		zap.String("collection", ut.req.GetCollectionName()),
		zap.String("partition", ut.req.GetPartitionName()),
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", ut.ID()),
		zap.Duration("get cache duration", getCacheDur),
		zap.Duration("get msgStream duration", getMsgStreamDur))

	// start to repack insert data
	var msgs []message.MutableMessage
	if ut.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result, ut.ezID)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result, ut.partitionKeys, ut.ezID)
	}
	if err != nil {
		log.Warn("assign segmentID and repack insert data failed", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	return msgs, nil
}

func (ut *upsertTaskByStreamingService) packDeleteMessage(ctx context.Context) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy deleteExecute upsert %d", ut.ID()))
	collID := ut.upsertMsg.DeleteMsg.CollectionID
	ut.upsertMsg.DeleteMsg.PrimaryKeys = ut.oldIds
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collID))
	// hash primary keys to channels
	vChannels, err := ut.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed when deleteExecute", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	result, numRows, err := repackDeleteMsgByHash(
		ctx,
		ut.upsertMsg.DeleteMsg.PrimaryKeys,
		vChannels, ut.idAllocator,
		ut.BeginTs(),
		ut.upsertMsg.DeleteMsg.CollectionID, ut.upsertMsg.DeleteMsg.CollectionName,
		ut.upsertMsg.DeleteMsg.PartitionID, ut.upsertMsg.DeleteMsg.PartitionName,
		ut.req.GetDbName(),
	)
	if err != nil {
		return nil, err
	}

	cipher, err := getCipher(ut.ezID)
	if err != nil {
		return nil, err
	}

	var msgs []message.MutableMessage
	for hashKey, deleteMsgs := range result {
		vchannel := vChannels[hashKey]
		for _, deleteMsg := range deleteMsgs {
			msg, err := message.NewDeleteMessageBuilderV1().
				WithHeader(&message.DeleteMessageHeader{
					CollectionId: ut.upsertMsg.DeleteMsg.CollectionID,
				}).
				WithBody(deleteMsg.DeleteRequest).
				WithVChannel(vchannel).
				WithCipher(cipher, ut.ezID).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
		}
	}

	log.Debug("Proxy Upsert deleteExecute done",
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", vChannels),
		zap.Int64("taskID", ut.ID()),
		zap.Int64("numRows", numRows),
		zap.Duration("prepare duration", tr.ElapseSpan()))

	return msgs, nil
}
