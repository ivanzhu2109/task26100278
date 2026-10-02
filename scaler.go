// Package autoscaler 是一个离线自动扩容决策库，由托管平台直接调用。
// 配置、指标与扩容记录全部保存在进程内存中。
package autoscaler

import (
	"fmt"
	"sort"
)

// 固定的扩容策略常量。
const (
	InitialVCPU     = 2   // 每个部署的初始 vCPU
	ScaleStep       = 2   // 每次扩容增加的 vCPU
	CooldownSeconds = 600 // 同一部署两次扩容的最小间隔
	MetricTTL       = 300 // CPU 指标的有效时长（秒）
	HighThreshold   = 80  // CPU 严格大于该百分比才算高位
)

// DeploymentConfig 是部署配置，初始化时一次给定，之后不修改。
type DeploymentConfig struct {
	ID               string
	SustainedSeconds int64 // 高位持续时长，取值 [60, 900]
}

// Metric 是一条带时间戳的 CPU 使用率指标（百分比）。
type Metric struct {
	DeploymentID string
	Timestamp    int64
	CPUPercent   float64
}

// StateChange 是一条带时间戳的运行状态变化。
type StateChange struct {
	DeploymentID string
	Timestamp    int64
	Running      bool
}

// ScaleRecord 是一条已确认的扩容记录。
type ScaleRecord struct {
	Timestamp int64
	NewVCPU   int
}

// Engine 保存全部内存状态：配置、原始输入与已确认结果。
type Engine struct {
	configs   map[string]DeploymentConfig
	metrics   map[string]map[int64]float64 // 部署 -> 时间戳 -> CPU（去重）
	states    map[string]map[int64]bool    // 部署 -> 时间戳 -> 运行状态（去重）
	watermark int64
	vcpu      map[string]int
	records   map[string][]ScaleRecord
}

// New 以给定配置创建 Engine，初始水位为 -1。
func New(configs []DeploymentConfig) (*Engine, error) {
	e := &Engine{
		configs:   make(map[string]DeploymentConfig, len(configs)),
		metrics:   make(map[string]map[int64]float64),
		states:    make(map[string]map[int64]bool),
		watermark: -1,
		vcpu:      make(map[string]int, len(configs)),
		records:   make(map[string][]ScaleRecord, len(configs)),
	}
	for _, cfg := range configs {
		if cfg.ID == "" {
			return nil, fmt.Errorf("autoscaler: deployment ID must not be empty")
		}
		if cfg.SustainedSeconds < 60 || cfg.SustainedSeconds > 900 {
			return nil, fmt.Errorf("autoscaler: deployment %q: sustained seconds %d out of range [60, 900]", cfg.ID, cfg.SustainedSeconds)
		}
		if _, dup := e.configs[cfg.ID]; dup {
			return nil, fmt.Errorf("autoscaler: duplicate deployment %q", cfg.ID)
		}
		e.configs[cfg.ID] = cfg
		e.vcpu[cfg.ID] = InitialVCPU
	}
	return e, nil
}

// Submit 提交一批带时间戳的 CPU 指标或运行状态变化。
// 时间戳不大于当前水位的输入直接忽略；重复输入不改变结果。
// 同部署、同类、同时间的输入由调用方保证没有冲突值。
func (e *Engine) Submit(metrics []Metric, states []StateChange) {
	for _, m := range metrics {
		if m.Timestamp < 0 || m.Timestamp <= e.watermark {
			continue
		}
		if _, known := e.configs[m.DeploymentID]; !known {
			continue
		}
		slot := e.metrics[m.DeploymentID]
		if slot == nil {
			slot = make(map[int64]float64)
			e.metrics[m.DeploymentID] = slot
		}
		if _, dup := slot[m.Timestamp]; !dup {
			slot[m.Timestamp] = m.CPUPercent
		}
	}
	for _, s := range states {
		if s.Timestamp < 0 || s.Timestamp <= e.watermark {
			continue
		}
		if _, known := e.configs[s.DeploymentID]; !known {
			continue
		}
		slot := e.states[s.DeploymentID]
		if slot == nil {
			slot = make(map[int64]bool)
			e.states[s.DeploymentID] = slot
		}
		if _, dup := slot[s.Timestamp]; !dup {
			slot[s.Timestamp] = s.Running
		}
	}
}

// Advance 推进确认水位，只能向前推进。
// 推进后确认截至该秒的全部应发生事件，包括没有输入恰好落在触发时刻的事件。
func (e *Engine) Advance(watermark int64) {
	if watermark <= e.watermark {
		return
	}
	e.watermark = watermark
	for id, cfg := range e.configs {
		vcpu, records := simulate(cfg, sortedMetrics(e.metrics[id]), sortedStates(e.states[id]), watermark)
		e.vcpu[id] = vcpu
		e.records[id] = records
	}
}

// Query 查询指定部署当前 vCPU 和已确认扩容记录（仅水位以内）。
func (e *Engine) Query(deploymentID string) (vcpu int, records []ScaleRecord, ok bool) {
	if _, known := e.configs[deploymentID]; !known {
		return 0, nil, false
	}
	recs := e.records[deploymentID]
	out := make([]ScaleRecord, len(recs))
	copy(out, recs)
	return e.vcpu[deploymentID], out, true
}

func sortedMetrics(slot map[int64]float64) []Metric {
	if len(slot) == 0 {
		return nil
	}
	ts := make([]int64, 0, len(slot))
	for t := range slot {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	out := make([]Metric, len(ts))
	for i, t := range ts {
		out[i] = Metric{Timestamp: t, CPUPercent: slot[t]}
	}
	return out
}

func sortedStates(slot map[int64]bool) []StateChange {
	if len(slot) == 0 {
		return nil
	}
	ts := make([]int64, 0, len(slot))
	for t := range slot {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	out := make([]StateChange, len(ts))
	for i, t := range ts {
		out[i] = StateChange{Timestamp: t, Running: slot[t]}
	}
	return out
}
