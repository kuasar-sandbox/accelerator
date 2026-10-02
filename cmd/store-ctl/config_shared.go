package main

import storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"

// Compatibility aliases keep existing store-ctl commands source-compatible
// while pkg/store/config is the single owner of the Store configuration model.
type Config = storeconfig.Config
type FSConfig = storeconfig.FSConfig
type S3Config = storeconfig.S3Config
type S3TLSConfig = storeconfig.S3TLSConfig
type GenerationsConfig = storeconfig.GenerationsConfig
type GenerationFileConfig = storeconfig.GenerationFileConfig
type GenerationS3Config = storeconfig.GenerationS3Config

var LoadConfig = storeconfig.LoadConfig
