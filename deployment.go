package autoscaler

const (
	// metricTTL 是 CPU 指标的有效时长（秒），有效期不包含失效时刻。
	metricTTL = 300
	// cooldown 是同一部署两次扩容的最小间隔（秒）。
	cooldown = 600
	// scaleStep 是每次扩容增加的 vCPU 数。
	scaleStep = 2
	// initialVCPU 是部署初始分配的 vCPU 数。
	initialVCPU = 2
	// highThreshold 是 CPU 高位阈值，严格大于才算高位。
	highThreshold = 80.0
)

// metricState 描述当前生效的 CPU 指标。
type metricState struct {
	valid  bool
	high   bool
	expiry int64 // 失效时刻（不含），即指标时间戳 + metricTTL
}

// deployment 保存单个部署的决策状态机。
type deployment struct {
	cfg Config

	vcpu      int
	running   bool
	metric    metricState
	inStreak  bool  // 是否处于连续高位中
	highSince int64 // 连续高位起始时刻
	lastScale int64 // 上次扩容时刻，-1 表示从未扩容
	watermark int64
	records   []ScaleRecord

	// 待处理输入，时间戳均大于当前水位；同键重复提交的值保证一致。
	pendingMetrics  map[int64]float64
	pendingStatuses map[int64]bool
}

func newDeployment(cfg Config) *deployment {
	return &deployment{
		cfg:             cfg,
		vcpu:            initialVCPU,
		running:         true,
		lastScale:       -1,
		watermark:       -1,
		pendingMetrics:  map[int64]float64{},
		pendingStatuses: map[int64]bool{},
	}
}

// submit 缓存一条时间戳大于当前水位的输入。
func (d *deployment) submit(in Input) {
	if in.Timestamp <= d.watermark {
		return
	}
	if in.CPU != nil {
		d.pendingMetrics[in.Timestamp] = *in.CPU
	}
	if in.Running != nil {
		d.pendingStatuses[in.Timestamp] = *in.Running
	}
}

// nextScaleTime 计算当前连续高位中下一次扩容的最早整数秒。
func (d *deployment) nextScaleTime() int64 {
	t := d.highSince + d.cfg.Duration
	if d.lastScale >= 0 && d.lastScale+cooldown > t {
		t = d.lastScale + cooldown
	}
	return t
}

// nextEventTime 返回不超过 limit 的下一个需要处理的时刻。
func (d *deployment) nextEventTime(limit int64) (int64, bool) {
	best := limit + 1
	found := false
	consider := func(t int64) {
		if t <= limit && t < best {
			best = t
			found = true
		}
	}
	for ts := range d.pendingStatuses {
		consider(ts)
	}
	for ts := range d.pendingMetrics {
		consider(ts)
	}
	if d.metric.valid {
		consider(d.metric.expiry)
	}
	if d.inStreak {
		consider(d.nextScaleTime())
	}
	return best, found
}

// processAt 处理时刻 t 的全部事件，顺序为：
// 运行状态变化 -> 新指标 -> 指标失效 -> 连续高位与扩容判断。
func (d *deployment) processAt(t int64) {
	if running, ok := d.pendingStatuses[t]; ok {
		delete(d.pendingStatuses, t)
		d.running = running
		if running {
			// 恢复运行后只能使用时间戳不早于恢复时刻的指标，
			// 此前的旧指标一律作废。
			d.metric.valid = false
		}
	}
	if cpu, ok := d.pendingMetrics[t]; ok {
		delete(d.pendingMetrics, t)
		d.metric = metricState{valid: true, high: cpu > highThreshold, expiry: t + metricTTL}
	}
	if d.metric.valid && d.metric.expiry <= t {
		d.metric.valid = false
	}

	high := d.running && d.metric.valid && d.metric.high
	if high {
		if !d.inStreak {
			d.inStreak = true
			d.highSince = t
		}
	} else {
		d.inStreak = false
	}

	if d.inStreak && t-d.highSince >= d.cfg.Duration &&
		(d.lastScale < 0 || t-d.lastScale >= cooldown) {
		d.vcpu += scaleStep
		d.lastScale = t
		d.records = append(d.records, ScaleRecord{Timestamp: t, VCPU: d.vcpu})
	}
}

// advanceTo 确认截至 W 的全部应发生事件。
func (d *deployment) advanceTo(W int64) {
	if W <= d.watermark {
		return
	}
	for {
		t, ok := d.nextEventTime(W)
		if !ok {
			break
		}
		d.processAt(t)
	}
	d.watermark = W
}
