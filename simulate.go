package autoscaler

// simulate 从原始输入确定性重放某部署截至水位（含）的全部事件，
// 返回该部署当前 vCPU 与已确认扩容记录。
//
// 同一时刻的处理顺序：运行状态变化 -> 新指标 -> 指标失效 -> 扩容判定。
// 扩容发生在持续时长已满足、冷却已结束且仍处于有效高位的最早整数秒，
// 不要求该时刻存在输入。结果只取决于输入集合与水位，与提交批次无关。
func simulate(cfg DeploymentConfig, metrics []Metric, states []StateChange, watermark int64) (int, []ScaleRecord) {
	vcpu := InitialVCPU
	running := true
	lastScale := int64(-1)  // 上次扩容时刻，-1 表示从未扩容
	highStart := int64(-1)  // 连续高位起点，-1 表示当前不处于连续高位
	hasMetric := false      // 当前是否持有有效指标
	metricHigh := false     // 当前指标是否处于高位
	metricUntil := int64(0) // 当前指标失效时刻（不含）
	var records []ScaleRecord

	mi, si := 0, 0
	for {
		// 找出下一个待处理时刻：输入事件、指标失效、应发生的扩容。
		next := int64(-1)
		consider := func(t int64) {
			if t <= watermark && (next == -1 || t < next) {
				next = t
			}
		}
		if mi < len(metrics) {
			consider(metrics[mi].Timestamp)
		}
		if si < len(states) {
			consider(states[si].Timestamp)
		}
		if hasMetric {
			consider(metricUntil)
		}
		if running && hasMetric && metricHigh && highStart >= 0 {
			cand := highStart + cfg.SustainedSeconds
			if lastScale >= 0 && lastScale+CooldownSeconds > cand {
				cand = lastScale + CooldownSeconds
			}
			// 失效时刻不含在有效期内，候选必须严格早于失效时刻。
			if cand < metricUntil {
				consider(cand)
			}
		}
		if next == -1 {
			break
		}
		t := next

		// 1. 运行状态变化。停止清空连续高位与当前指标（不回收 vCPU、
		//    不清除上次扩容时间）；恢复后只能使用不早于恢复时刻的指标。
		for si < len(states) && states[si].Timestamp == t {
			st := states[si]
			si++
			if st.Running {
				running = true
				// 恢复时刻之前（含停止期间）的旧指标一律不可用；
				// 此时持有的任何指标时间戳都必然早于恢复时刻。
				hasMetric = false
			} else {
				running = false
				hasMetric = false
				highStart = -1
			}
		}

		// 2. 新指标。恰在旧指标失效时到达视为连续覆盖：新旧皆高位且
		//    未停止时，连续高位起点保持不变。
		for mi < len(metrics) && metrics[mi].Timestamp == t {
			m := metrics[mi]
			mi++
			continuous := running && hasMetric && metricHigh && highStart >= 0 && t <= metricUntil
			hasMetric = true
			metricHigh = m.CPUPercent > HighThreshold
			metricUntil = t + MetricTTL
			if metricHigh && running {
				if !continuous {
					highStart = t
				}
			} else {
				highStart = -1
			}
		}

		// 3. 指标失效：有效期不包含失效时刻。
		if hasMetric && metricUntil == t {
			hasMetric = false
			highStart = -1
		}

		// 4. 扩容判定。扩容不清空连续高位时长。
		if running && hasMetric && metricHigh && highStart >= 0 &&
			t-highStart >= cfg.SustainedSeconds &&
			(lastScale < 0 || t-lastScale >= CooldownSeconds) {
			vcpu += ScaleStep
			lastScale = t
			records = append(records, ScaleRecord{Timestamp: t, NewVCPU: vcpu})
		}
	}
	return vcpu, records
}
