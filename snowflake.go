package main

import (
	"errors"
	"fmt"
	"github.com/js179/logf"
	"sync"
	"time"
)

const (
	timestampBits   = uint(41) // 时间戳占用位数
	workerBits      = uint(4)  // 工作节点占用位数
	datacenterBits  = uint(6)  // 数据节点占用位数
	seqBits         = uint(12) //序列号占用位数
	timestampMax    = int64(-1 ^ (-1 << timestampBits))
	workerMax       = int64(-1 ^ (-1 << workerBits))
	datacenterMax   = int64(-1 ^ (-1 << datacenterBits))
	seqMax          = int64(-1 ^ (-1 << seqBits))
	workerShift     = seqBits // 偏移量
	datacenterShift = seqBits + workerBits
	timestampShift  = seqBits + workerBits + datacenterBits
)

type Snowflake struct {
	sync.Mutex
	epoch      int64 // 开始日期时间戳
	timestamp  int64 // 时间戳
	worker     int64 // 工作节点id
	datacenter int64 // 数据节点id
	seq        int64 // 序列号
}

func (s *Snowflake) SetWorker(worker int64) {
	if worker > workerMax {
		logf.Errorf("worker must be between 0 and %d", workerMax-1)
		s.worker = 0
	} else {
		s.worker = worker
	}
}

func (s *Snowflake) SetDatacenter(d int64) {
	if d > datacenterMax {
		logf.Errorf("datacenter must be between 0 and %d", workerMax-1)
		s.worker = 0
	} else {
		s.datacenter = d
	}
}

func (s *Snowflake) SetEpoch(epoch int64) {
	if epoch > time.Now().UnixMilli() || epoch < 0 {
		epoch = time.Now().UnixMilli()
	}
	s.epoch = epoch
}

func (s *Snowflake) CreateID() (int64, error) {
	s.Lock()
	defer s.Unlock()

	now := time.Now().UnixMilli()
	if now == s.timestamp {
		s.seq = (s.seq + 1) & seqMax
		// 当前毫秒内的seq已经全部占用，等待下一毫秒内再生成
		if s.seq == 0 {
			for now < s.timestamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		s.seq = 0
	}

	t := now - s.epoch
	if t > timestampMax {
		return -1, errors.New(fmt.Sprintf("epoch must be between 0 and %d", timestampMax-1))
	}
	s.timestamp = now
	return (t << timestampShift) | (s.datacenter << datacenterShift) | (s.worker << workerShift) | s.seq, nil
}

var snowflake *Snowflake

func New(work, d, epoch int64) {
	snowflake.SetWorker(work)
	snowflake.SetDatacenter(d)
	snowflake.SetEpoch(epoch)
}

func init() {
	New(4, 6, 1687752676742)
}

func ID() int64 {
	id, _ := snowflake.CreateID()
	return id
}
