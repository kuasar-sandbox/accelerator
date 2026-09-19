package main

import (
	"context"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/transfer"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"io"
)

func writeTransfer(ctx context.Context, cfg *manifest.Config, key [32]byte, src sparse.Source, mode string, out io.Writer, name string, storeOut bool, extra []byte, codec tarstream.Codec, required bool, finish func() error) (transfer.Result, error) {
	return transfer.Write(ctx, cfg, key, src, transfer.WriteOptions{Mode: mode, Output: out, Name: name, Store: storeOut, ExtraSalt: extra, Codec: codec, RequireLocalEncryption: required, BeforeCommit: finish})
}
