package autoscaler

// Config 是部署的扩容配置，初始化时一次给定，之后不修改。
type Config struct {
	DeploymentID string
	// Duration 为持续时长（秒），取值 60 至 900 的整数。
	Duration int64
}

// Input 是一条带时间戳的输入：CPU 指标或运行状态变化，二者必居其一。
type Input struct {
	DeploymentID string
	Timestamp    int64
	// CPU 非空时表示 CPU 使用率指标（百分比）。
	CPU *float64
	// Running 非空时表示运行状态变化。
	Running *bool
}

// ScaleRecord 是一条已确认的扩容记录。
type ScaleRecord struct {
	Timestamp int64
	VCPU      int
}

// Snapshot 是查询结果：当前 vCPU 与已确认扩容记录。
type Snapshot struct {
	VCPU    int
	Records []ScaleRecord
}
