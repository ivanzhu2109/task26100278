package autoscaler

import "fmt"

const (
	minDuration = 60
	maxDuration = 900
)

// Engine 是离线自动扩容决策库，全部状态保存在进程内存中。
type Engine struct {
	deployments map[string]*deployment
	watermark   int64
}

// NewEngine 以给定部署配置创建引擎，初始水位为 -1。
// 持续时长必须是 60 至 900 的整数秒，部署 ID 不得重复。
func NewEngine(configs []Config) (*Engine, error) {
	e := &Engine{deployments: map[string]*deployment{}, watermark: -1}
	for _, cfg := range configs {
		if cfg.DeploymentID == "" {
			return nil, fmt.Errorf("autoscaler: empty deployment id")
		}
		if cfg.Duration < minDuration || cfg.Duration > maxDuration {
			return nil, fmt.Errorf("autoscaler: deployment %q: duration %d out of range [%d, %d]",
				cfg.DeploymentID, cfg.Duration, minDuration, maxDuration)
		}
		if _, dup := e.deployments[cfg.DeploymentID]; dup {
			return nil, fmt.Errorf("autoscaler: duplicate deployment id %q", cfg.DeploymentID)
		}
		e.deployments[cfg.DeploymentID] = newDeployment(cfg)
	}
	return e, nil
}

// Submit 提交一批带时间戳的 CPU 指标或运行状态变化。
// 输入允许乱序和重复；时间戳不大于当前水位的输入直接忽略。
func (e *Engine) Submit(inputs []Input) {
	for _, in := range inputs {
		if in.Timestamp < 0 || in.Timestamp <= e.watermark {
			continue
		}
		if (in.CPU == nil) == (in.Running == nil) {
			continue
		}
		d, ok := e.deployments[in.DeploymentID]
		if !ok {
			continue
		}
		d.submit(in)
	}
}

// AdvanceWatermark 推进确认水位，确认截至该秒的全部应发生事件。
// 水位只能向前推进，小于等于当前水位的推进被忽略。
func (e *Engine) AdvanceWatermark(ts int64) {
	if ts <= e.watermark {
		return
	}
	for _, d := range e.deployments {
		d.advanceTo(ts)
	}
	e.watermark = ts
}

// Query 查询指定部署当前 vCPU 和已确认扩容记录，结果只包含水位以内的部分。
func (e *Engine) Query(deploymentID string) (Snapshot, bool) {
	d, ok := e.deployments[deploymentID]
	if !ok {
		return Snapshot{}, false
	}
	records := make([]ScaleRecord, len(d.records))
	copy(records, d.records)
	return Snapshot{VCPU: d.vcpu, Records: records}, true
}
