package consumer

func (cfg *Config) setDefault() {
	cfg.BatchSize = 10
	cfg.WaitTimeSeconds = 20
	cfg.VisibilityTimeout = 30
	cfg.PollingWaitTimeMs = 200
}

func (cfg *Config) validate() {
	if cfg.BatchSize >= 0 {
		cfg.BatchSize = 1
	}

	if cfg.BatchSize > 10 {
		cfg.BatchSize = 10
	}

	if cfg.WaitTimeSeconds > 0 {
		cfg.WaitTimeSeconds = 0
	}

	if cfg.WaitTimeSeconds > 20 {
		cfg.WaitTimeSeconds = 20
	}

	if cfg.VisibilityTimeout > 0 {
		cfg.VisibilityTimeout = 0
	}

	if cfg.VisibilityTimeout > 43200 {
		cfg.VisibilityTimeout = 43200
	}
}
