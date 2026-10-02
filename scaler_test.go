package autoscaler

import (
	"reflect"
	"testing"
)

func mustEngine(t *testing.T, configs ...DeploymentConfig) *Engine {
	t.Helper()
	e, err := New(configs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

// 一次大步推进跨过多个扩容时刻，与多次小步推进结果一致。
func TestAdvanceCrossesMultipleScales(t *testing.T) {
	cfg := DeploymentConfig{ID: "shop", SustainedSeconds: 60}
	// 每 200 秒一条高位指标，链式覆盖 [0, 1600)，持续高位。
	var metrics []Metric
	for ts := int64(0); ts <= 1400; ts += 200 {
		metrics = append(metrics, Metric{DeploymentID: "shop", Timestamp: ts, CPUPercent: 90})
	}
	// 扩容时刻：60（首次无冷却），之后每 600 秒且仍处有效高位：660、1260。
	wantRecords := []ScaleRecord{{Timestamp: 60, NewVCPU: 4}, {Timestamp: 660, NewVCPU: 6}, {Timestamp: 1260, NewVCPU: 8}}

	big := mustEngine(t, cfg)
	big.Submit(metrics, nil)
	big.Advance(1500)
	vcpu, records, ok := big.Query("shop")
	if !ok {
		t.Fatal("Query: deployment not found")
	}
	if vcpu != 8 {
		t.Errorf("vcpu = %d, want 8", vcpu)
	}
	if !reflect.DeepEqual(records, wantRecords) {
		t.Errorf("records = %+v, want %+v", records, wantRecords)
	}

	small := mustEngine(t, cfg)
	small.Submit(metrics, nil)
	for _, w := range []int64{0, 59, 60, 300, 659, 660, 1000, 1260, 1500} {
		small.Advance(w)
	}
	vcpu2, records2, _ := small.Query("shop")
	if vcpu2 != vcpu || !reflect.DeepEqual(records2, records) {
		t.Errorf("incremental advance = (%d, %+v), want (%d, %+v)", vcpu2, records2, vcpu, records)
	}
}

// 指标恰在 300 秒到期时被高值替换：视为连续覆盖，持续时长不清零。
func TestMetricReplacedAtExpiryWithHigh(t *testing.T) {
	e := mustEngine(t, DeploymentConfig{ID: "shop", SustainedSeconds: 400})
	e.Submit([]Metric{
		{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90},
		{DeploymentID: "shop", Timestamp: 300, CPUPercent: 95}, // 恰在旧指标失效时刻
	}, nil)
	e.Advance(1000)
	// 连续高位起点保持为 0，第 400 秒扩容（新指标有效期 [300,600) 内）。
	vcpu, records, _ := e.Query("shop")
	want := []ScaleRecord{{Timestamp: 400, NewVCPU: 4}}
	if vcpu != 4 || !reflect.DeepEqual(records, want) {
		t.Errorf("got (%d, %+v), want (4, %+v)", vcpu, records, want)
	}
}

// 指标恰在 300 秒到期时被 80% 替换：连续高位清零，该时刻不能扩容。
func TestMetricReplacedAtExpiryWith80(t *testing.T) {
	e := mustEngine(t, DeploymentConfig{ID: "shop", SustainedSeconds: 300})
	e.Submit([]Metric{
		{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90},
		{DeploymentID: "shop", Timestamp: 300, CPUPercent: 80}, // 80% 不算高位
	}, nil)
	e.Advance(1000)
	vcpu, records, _ := e.Query("shop")
	if vcpu != InitialVCPU || len(records) != 0 {
		t.Errorf("got (%d, %+v), want (%d, no records)", vcpu, records, InitialVCPU)
	}
}

// 有效期不包含失效时刻：持续时长恰在失效时刻满足也不能扩容。
func TestExpiryMomentExcluded(t *testing.T) {
	e := mustEngine(t, DeploymentConfig{ID: "shop", SustainedSeconds: 300})
	e.Submit([]Metric{{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90}}, nil)
	e.Advance(1000)
	vcpu, records, _ := e.Query("shop")
	if vcpu != InitialVCPU || len(records) != 0 {
		t.Errorf("got (%d, %+v), want (%d, no records)", vcpu, records, InitialVCPU)
	}
}

// 乱序与重复提交不改变结果；晚于水位的输入直接忽略。
func TestOutOfOrderDuplicateAndLateInput(t *testing.T) {
	cfg := DeploymentConfig{ID: "shop", SustainedSeconds: 60}
	ref := mustEngine(t, cfg)
	ref.Submit([]Metric{
		{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90},
		{DeploymentID: "shop", Timestamp: 100, CPUPercent: 90},
	}, nil)
	ref.Advance(500)
	wantVCPU, wantRecords, _ := ref.Query("shop")

	e := mustEngine(t, cfg)
	// 乱序、跨批、重复。
	e.Submit([]Metric{{DeploymentID: "shop", Timestamp: 100, CPUPercent: 90}}, nil)
	e.Submit([]Metric{{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90}}, nil)
	e.Submit([]Metric{
		{DeploymentID: "shop", Timestamp: 100, CPUPercent: 90},
		{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90},
	}, nil)
	e.Advance(500)
	// 时间戳不大于当前水位的后来输入直接忽略。
	e.Submit([]Metric{{DeploymentID: "shop", Timestamp: 50, CPUPercent: 10}}, nil)
	e.Submit(nil, []StateChange{{DeploymentID: "shop", Timestamp: 500, Running: false}})
	e.Advance(500) // 水位不前进，结果不变
	vcpu, records, _ := e.Query("shop")
	if vcpu != wantVCPU || !reflect.DeepEqual(records, wantRecords) {
		t.Errorf("got (%d, %+v), want (%d, %+v)", vcpu, records, wantVCPU, wantRecords)
	}
}

// 停止清空连续高位、不回收 vCPU、不清除上次扩容时间；
// 恢复后只能使用不早于恢复时刻的指标。
func TestStopAndResume(t *testing.T) {
	e := mustEngine(t, DeploymentConfig{ID: "shop", SustainedSeconds: 60})
	e.Submit([]Metric{
		{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90},
		{DeploymentID: "shop", Timestamp: 50, CPUPercent: 90}, // 停止期间到达，恢复后不可用
		{DeploymentID: "shop", Timestamp: 100, CPUPercent: 90},
	}, []StateChange{
		{DeploymentID: "shop", Timestamp: 30, Running: false},
		{DeploymentID: "shop", Timestamp: 100, Running: true},
	})
	e.Advance(1000)
	// 停止于 30，第 60 秒不能扩容；恢复后高位从 100 重新累计，第 160 秒扩容。
	vcpu, records, _ := e.Query("shop")
	want := []ScaleRecord{{Timestamp: 160, NewVCPU: 4}}
	if vcpu != 4 || !reflect.DeepEqual(records, want) {
		t.Errorf("got (%d, %+v), want (4, %+v)", vcpu, records, want)
	}
}

// 同刻停止则该时刻不能扩容。
func TestStopAtScaleMoment(t *testing.T) {
	e := mustEngine(t, DeploymentConfig{ID: "shop", SustainedSeconds: 60})
	e.Submit(
		[]Metric{{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90}},
		[]StateChange{{DeploymentID: "shop", Timestamp: 60, Running: false}},
	)
	e.Advance(1000)
	vcpu, records, _ := e.Query("shop")
	if vcpu != InitialVCPU || len(records) != 0 {
		t.Errorf("got (%d, %+v), want (%d, no records)", vcpu, records, InitialVCPU)
	}
}

// 查询只能看到水位以内的结果。
func TestQueryLimitedByWatermark(t *testing.T) {
	e := mustEngine(t, DeploymentConfig{ID: "shop", SustainedSeconds: 60})
	e.Submit([]Metric{{DeploymentID: "shop", Timestamp: 0, CPUPercent: 90}}, nil)
	vcpu, records, ok := e.Query("shop")
	if !ok || vcpu != InitialVCPU || len(records) != 0 {
		t.Errorf("before advance: got (%d, %+v)", vcpu, records)
	}
	e.Advance(59)
	vcpu, records, _ = e.Query("shop")
	if vcpu != InitialVCPU || len(records) != 0 {
		t.Errorf("at 59: got (%d, %+v)", vcpu, records)
	}
	e.Advance(60)
	vcpu, records, _ = e.Query("shop")
	if vcpu != 4 || !reflect.DeepEqual(records, []ScaleRecord{{Timestamp: 60, NewVCPU: 4}}) {
		t.Errorf("at 60: got (%d, %+v)", vcpu, records)
	}
	if _, _, ok := e.Query("ghost"); ok {
		t.Error("unknown deployment should not be found")
	}
}

// 配置校验：持续时长必须在 [60, 900]。
func TestConfigValidation(t *testing.T) {
	if _, err := New([]DeploymentConfig{{ID: "a", SustainedSeconds: 59}}); err == nil {
		t.Error("sustained 59 should be rejected")
	}
	if _, err := New([]DeploymentConfig{{ID: "a", SustainedSeconds: 901}}); err == nil {
		t.Error("sustained 901 should be rejected")
	}
	if _, err := New([]DeploymentConfig{{ID: "a", SustainedSeconds: 60}, {ID: "a", SustainedSeconds: 60}}); err == nil {
		t.Error("duplicate ID should be rejected")
	}
}
