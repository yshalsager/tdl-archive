package main

import (
	"fmt"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

type config struct {
	Group          string   `yaml:"group"`
	MediaDir       string   `yaml:"media_dir"`
	DownloadMedia  bool     `yaml:"download_media"`
	MediaMIMETypes []string `yaml:"media_mime_types"`
	FetchBatchSize int      `yaml:"fetch_batch_size"`
	FetchLimit     int      `yaml:"fetch_limit"`
	UseTakeout     bool     `yaml:"use_takeout"`
	JSONDump       bool     `yaml:"json_dump"`
}

func loadConfig(path string) (config, error) {
	cfg := config{MediaDir: "media", FetchBatchSize: 100}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if !filepath.IsAbs(cfg.MediaDir) {
		cfg.MediaDir = filepath.Join(filepath.Dir(path), cfg.MediaDir)
	}
	return cfg, nil
}

func (c config) validate() error {
	if c.Group == "" {
		return fmt.Errorf("group is required")
	}
	if c.FetchBatchSize < 1 {
		return fmt.Errorf("fetch_batch_size must be positive")
	}
	if c.FetchLimit < 0 {
		return fmt.Errorf("fetch_limit must be non-negative")
	}
	return nil
}
