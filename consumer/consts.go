package consumer

const (
	BatchSizeLimit                    int64  = 10
	ReceiveMessageWaitSecondsLimit    int64  = 20
	DefaultReceiveVisibilityTimeout   int64  = 30
	DefaultTerminateVisibilityTimeout int64  = 0
	INFO                              string = "INFO "
	WARN                              string = "WARN "
	ERROR                             string = "ERROR"
)
