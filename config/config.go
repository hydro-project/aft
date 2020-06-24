package config

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type AftConfig struct {
	ConsistencyType string   `yaml:"consistencyType"`
	StorageType     string   `yaml:"storageType"`
	IpAddress       string   `yaml:"ipAddress"`
	ElbAddress      string   `yaml:"elbAddress"`
	ReplicaList     []string `yaml:"replicaList"`
	ManagerAddress  string   `yaml:"managerAddress"`
}

func ParseConfig() *AftConfig {
	home := os.Getenv("GOPATH")
	confPath := filepath.Join(home, "src", "github.com", "vsreekanti", "aft", "config", "aft-config.yml")

	bts, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatal("Unable to read aft-config.yml. Please make sure that the config is properly configured and retry:\n%v", err)
		os.Exit(1)
	}

	var config AftConfig
	err = yaml.Unmarshal(bts, &config)
	if err != nil {
		log.Fatal("Unable to correctly parse aft-config.yml. Please check the config file and retry:\n%v", err)
		os.Exit(1)
	}

	return &config
}
