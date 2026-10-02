package autoscaler

import (
	"reflect"
	"testing"
)

func metric(id string, ts int64, cpu float64) Input {
	return Input{DeploymentID: id, Timestamp: ts, CPU: &cpu}
}

func status(id string, ts int64, running bool) Input {
	return Input{DeploymentID: id, Timestamp: ts, Running: &running}
}

func newEngine(t *testing.T, cfgs ...Config) *Engine {
	t.Helper()
	e, err := NewEngine(cfgs)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

func mustQuery(t *testing.T, e *Engine, id string) Snapshot {
	t.Helper()
	snap, ok := e.Query(id)
	if !ok {
		t.Fatalf("Query(%q): not found", id)
	}
	return snap
}

func assertSnapshot(t *testing.T, e *Engine, id string, vcpu int, records []ScaleRecord) {
	t.Helper()
	snap := mustQuery(t, e, id)
	if snap.VCPU != vcpu {
		t.Fatalf("vCPU = %d, want %d", snap.VCPU, vcpu)
	}
	if len(records) == 0 && len(snap.Records) == 0 {
		return
	}
	if !reflect.DeepEqual(snap.Records, records) {
		t.Fatalf("records = %+v, want %+v", snap.Records, records)
	}
}

// 一次推进跨过多个扩容时刻：持续高位期间每隔 600 秒再次扩容。
func TestSingleAdvanceCrossesMultipleScaleUps(t *testing.T) {
	cfg := Config{DeploymentID: "a", Duration: 60}
	inputs := []Input{
		metric("a", 0, 90), metric("a", 300, 90),
		metric("a", 600, 90), metric("a", 900, 90),
	}
	want := []ScaleRecord{{Timestamp: 60, VCPU: 4}, {Timestamp: 660, VCPU: 6}}

	big := newEngine(t, cfg)
	big.Submit(inputs)
	big.AdvanceWatermark(1000)
	assertSnapshot(t, big, "a", 6, want)

	// 多次小步推进必须得到相同结果。
	small := newEngine(t, cfg)
	small.Submit(inputs)
	for _, w := range []int64{0, 59, 60, 61, 300, 659, 660, 661, 1000} {
		small.AdvanceWatermark(w)
	}
	assertSnapshot(t, small, "a", 6, want)
}

// 指标恰在 300 秒到期时被高值替换：视为连续覆盖，连续高位时长不清零。
func TestExpiryReplacedByHighValue(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90), metric("a", 300, 85), metric("a", 600, 90)})
	e.AdvanceWatermark(1000)
	assertSnapshot(t, e, "a", 6, []ScaleRecord{
		{Timestamp: 60, VCPU: 4},
		{Timestamp: 660, VCPU: 6},
	})
}

// 指标恰在 300 秒到期时被 80% 替换：不算高位，连续高位时长清零。
func TestExpiryReplacedByEightyPercent(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90), metric("a", 300, 80)})
	e.AdvanceWatermark(1000)
	assertSnapshot(t, e, "a", 4, []ScaleRecord{{Timestamp: 60, VCPU: 4}})
}

// 指标到期后没有新指标：连续高位在失效时刻终止。
func TestExpiryWithoutReplacement(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90)})
	e.AdvanceWatermark(1000)
	assertSnapshot(t, e, "a", 4, []ScaleRecord{{Timestamp: 60, VCPU: 4}})
}

// 乱序与重复提交不得改变结果。
func TestOutOfOrderAndDuplicateInputs(t *testing.T) {
	cfg := Config{DeploymentID: "a", Duration: 60}
	ordered := newEngine(t, cfg)
	ordered.Submit([]Input{
		metric("a", 0, 90), metric("a", 300, 90),
		metric("a", 600, 90), metric("a", 900, 90),
	})
	ordered.AdvanceWatermark(1000)

	shuffled := newEngine(t, cfg)
	shuffled.Submit([]Input{metric("a", 900, 90), metric("a", 300, 90)})
	shuffled.Submit([]Input{metric("a", 600, 90), metric("a", 300, 90), metric("a", 0, 90)})
	shuffled.Submit([]Input{metric("a", 0, 90), metric("a", 900, 90)})
	shuffled.AdvanceWatermark(1000)

	got := mustQuery(t, shuffled, "a")
	want := mustQuery(t, ordered, "a")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shuffled = %+v, want %+v", got, want)
	}
}

// 停止不回收 vCPU、不清除上次扩容时间；恢复后旧指标不可用，冷却仍然有效。
func TestStopAndResume(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{
		metric("a", 0, 90),
		status("a", 100, false),
		status("a", 200, true),
		metric("a", 250, 90), metric("a", 550, 90),
	})
	e.AdvanceWatermark(1000)
	// 首次扩容在 60；停机 100 清掉连续高位；200 恢复后 0 时刻的旧指标作废；
	// 250 起新的连续高位，但受上次扩容 600 秒冷却约束，下次扩容在 660。
	assertSnapshot(t, e, "a", 6, []ScaleRecord{
		{Timestamp: 60, VCPU: 4},
		{Timestamp: 660, VCPU: 6},
	})
}

// 同刻停止，则该时刻不能扩容。
func TestStopAtScaleMomentPreventsScaleUp(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90), status("a", 60, false)})
	e.AdvanceWatermark(100)
	assertSnapshot(t, e, "a", 2, nil)
}

// 同刻收到 80% 的指标，则该时刻不能扩容。
func TestEightyPercentAtScaleMomentPreventsScaleUp(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90), metric("a", 60, 80)})
	e.AdvanceWatermark(100)
	assertSnapshot(t, e, "a", 2, nil)
}

// 时间戳不大于当前水位的后来输入直接忽略。
func TestLateInputIgnored(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90)})
	e.AdvanceWatermark(100)
	e.Submit([]Input{metric("a", 50, 10), status("a", 80, false)})
	e.AdvanceWatermark(1000)
	assertSnapshot(t, e, "a", 4, []ScaleRecord{{Timestamp: 60, VCPU: 4}})
}

// 查询只能看到水位以内的结果。
func TestQuerySeesOnlyConfirmedResults(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90)})
	assertSnapshot(t, e, "a", 2, nil)
	e.AdvanceWatermark(59)
	assertSnapshot(t, e, "a", 2, nil)
	e.AdvanceWatermark(60)
	assertSnapshot(t, e, "a", 4, []ScaleRecord{{Timestamp: 60, VCPU: 4}})
}

// 水位只能向前推进。
func TestWatermarkOnlyMovesForward(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 60})
	e.Submit([]Input{metric("a", 0, 90)})
	e.AdvanceWatermark(100)
	e.AdvanceWatermark(50)
	assertSnapshot(t, e, "a", 4, []ScaleRecord{{Timestamp: 60, VCPU: 4}})
}

// 持续时长恰好在指标失效时刻满足时不扩容（有效期不含失效时刻）。
func TestDurationMetExactlyAtExpiry(t *testing.T) {
	e := newEngine(t, Config{DeploymentID: "a", Duration: 300})
	e.Submit([]Input{metric("a", 0, 90)})
	e.AdvanceWatermark(1000)
	assertSnapshot(t, e, "a", 2, nil)
}

// 配置校验。
func TestConfigValidation(t *testing.T) {
	if _, err := NewEngine([]Config{{DeploymentID: "a", Duration: 59}}); err == nil {
		t.Fatal("duration 59: want error")
	}
	if _, err := NewEngine([]Config{{DeploymentID: "a", Duration: 901}}); err == nil {
		t.Fatal("duration 901: want error")
	}
	if _, err := NewEngine([]Config{
		{DeploymentID: "a", Duration: 60},
		{DeploymentID: "a", Duration: 60},
	}); err == nil {
		t.Fatal("duplicate id: want error")
	}
	if _, err := NewEngine([]Config{
		{DeploymentID: "a", Duration: 60},
		{DeploymentID: "b", Duration: 900},
	}); err != nil {
		t.Fatalf("valid configs: %v", err)
	}
}

// 多部署互不影响。
func TestMultipleDeployments(t *testing.T) {
	e := newEngine(t,
		Config{DeploymentID: "a", Duration: 60},
		Config{DeploymentID: "b", Duration: 120},
	)
	e.Submit([]Input{metric("a", 0, 90), metric("b", 10, 90)})
	e.AdvanceWatermark(200)
	assertSnapshot(t, e, "a", 4, []ScaleRecord{{Timestamp: 60, VCPU: 4}})
	assertSnapshot(t, e, "b", 4, []ScaleRecord{{Timestamp: 130, VCPU: 4}})
}
