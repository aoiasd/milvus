package datacoord

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// channel stats meta saves stats binlog info
// used by text match (BM25)
type channelStatsMeta struct {
	sync.RWMutex
	ctx               context.Context
	catalog           metastore.DataCoordCatalog
	channelStatsInfos map[string]*datapb.ChannelStatsInfo // channel -> partition -> PartitionStatsInfo
}

func newChannelStatsMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*channelStatsMeta, error) {
	csm := &channelStatsMeta{
		RWMutex:           sync.RWMutex{},
		ctx:               ctx,
		catalog:           catalog,
		channelStatsInfos: make(map[string]*datapb.ChannelStatsInfo),
	}
	if err := csm.reloadFromKV(); err != nil {
		return nil, err
	}
	return csm, nil
}

func (m *channelStatsMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("channelStatsMeta-reloadFromKV")

	channelStatsInfos, err := m.catalog.ListChannelStatsInfo(m.ctx)
	if err != nil {
		return err
	}

	for _, info := range channelStatsInfos {
		m.channelStatsInfos[info.VChannel] = info
	}

	log.Info("DataCoord channelStatsMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *channelStatsMeta) addStatsInfo(collectionID int64, channel string, checkpoint *datapb.CheckPoint, binlogs []*datapb.FieldBinlog) (*datapb.ChannelStatsInfo, error) {
	info, ok := m.channelStatsInfos[channel]
	if !ok {
		return &datapb.ChannelStatsInfo{
			VChannel:     channel,
			CollectionID: collectionID,
			Statslogs:    binlogs,
			Checkpoint:   checkpoint,
		}, nil
	}

	newinfo := proto.Clone(info).(*datapb.ChannelStatsInfo)
	newinfo.Checkpoint = checkpoint
	binlogMap := lo.SliceToMap(binlogs, func(binlog *datapb.FieldBinlog) (int64, *datapb.FieldBinlog) { return binlog.GetFieldID(), binlog })
	for _, field := range newinfo.Statslogs {
		binlog, ok := binlogMap[field.GetFieldID()]
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("channel %s lack field %d binlog cause update failed", channel, field.GetFieldID())
		}
		binlog.Binlogs = append(binlog.Binlogs, field.GetBinlogs()...)

	}
	return newinfo, nil
}

func (m *channelStatsMeta) Update(collectionID int64, channel string, checkpoint *datapb.CheckPoint, binlogs []*datapb.FieldBinlog) error {
	record := timerecord.NewTimeRecorder("channelStatsMeta-Update")

	m.Lock()
	defer m.Unlock()
	newinfo, err := m.addStatsInfo(collectionID, channel, checkpoint, binlogs)
	if err != nil {
		// TODO ADD LOG WARN
		return err
	}

	err = m.catalog.SaveChannelStatsInfo(m.ctx, newinfo)
	if err != nil {
		// TODO ADD LOG WARN
		return err
	}

	m.channelStatsInfos[channel] = newinfo
	log.Debug("DataCoord channelStatsMeta update done", zap.Duration("duration", record.ElapseSpan()), zap.Any("checkpoint", checkpoint), zap.Any("new binlog", binlogs))
	return nil
}
