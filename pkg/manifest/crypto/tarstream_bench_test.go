package crypto

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func BenchmarkTarStreamCodecPrimitive4KiB(b *testing.B) {
	codec, _ := NewTarStreamCodec([32]byte{0x31})
	records, _ := codec.BindArtifact([32]byte{0x41})
	plaintext := bytes.Repeat([]byte{0x5a}, 4096)
	aad := bytes.Repeat([]byte{0xa5}, 128)
	sealed, _ := records.Encrypt(nil, plaintext, aad, 1)
	b.Run("encrypt", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		for i := 0; i < b.N; i++ {
			if _, err := records.Encrypt(make([]byte, 0, len(sealed)), plaintext, aad, uint64(i+1)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decrypt", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		buffer := make([]byte, len(sealed))
		for i := 0; i < b.N; i++ {
			copy(buffer, sealed)
			if _, err := records.DecryptInPlace(buffer, aad, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkEncryptedTarStream(b *testing.B) {
	codec, _ := NewTarStreamCodec([32]byte{0x32})
	body := bytes.Repeat([]byte{0x6b}, 32<<20)
	artifact, scheme, digest := writeTarArtifact(b, codec, "image", body, nil)
	plaintextArtifact, plaintextScheme, plaintextDigest := writeTarArtifact(b, nil, "image", body, nil)
	sequentialBody := body[:1<<20]
	sequentialArtifact, sequentialScheme, sequentialDigest := writeTarArtifact(b, codec, "image", sequentialBody, nil)

	b.Run("open-plaintext", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			source, _, err := tarstream.SourceAt(bytes.NewReader(plaintextArtifact), int64(len(plaintextArtifact)), "", tarstream.WithExpectedDigest(plaintextScheme, plaintextDigest))
			if err != nil {
				b.Fatal(err)
			}
			if closer, ok := source.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})

	b.Run("open-encrypted", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			source, _, err := tarstream.SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
			if err != nil {
				b.Fatal(err)
			}
			if closer, ok := source.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})

	b.Run("pipe-1MiB-full-validation", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(sequentialBody)))
		for i := 0; i < b.N; i++ {
			source, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(sequentialArtifact)}, "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(sequentialScheme, sequentialDigest))
			if err != nil {
				b.Fatal(err)
			}
			if err := consumeData(source); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("random-4KiB-miss", func(b *testing.B) {
		source, _, err := tarstream.SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", tarstream.WithCodec(codec, true))
		if err != nil {
			b.Fatal(err)
		}
		buffer := make([]byte, 4096)
		records := len(body) / len(buffer)
		b.ReportAllocs()
		b.SetBytes(int64(len(buffer)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offset := uint64((i*7919)%records) * uint64(len(buffer))
			if _, err := source.ReadAt(context.Background(), buffer, offset); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("random-1MiB", func(b *testing.B) {
		source, _, err := tarstream.SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", tarstream.WithCodec(codec, true))
		if err != nil {
			b.Fatal(err)
		}
		buffer := make([]byte, 1<<20)
		windows := len(body) / len(buffer)
		b.ReportAllocs()
		b.SetBytes(int64(len(buffer)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offset := uint64((i*13)%windows) * uint64(len(buffer))
			if _, err := source.ReadAt(context.Background(), buffer, offset); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("pipe-full-validation", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(body)))
		for i := 0; i < b.N; i++ {
			source, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(artifact)}, "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
			if err != nil {
				b.Fatal(err)
			}
			if err := consumeData(source); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkEncryptedTarStreamWrite(b *testing.B) {
	codec, _ := NewTarStreamCodec([32]byte{0x33})
	body := bytes.Repeat([]byte{0x7c}, 16<<20)
	dense, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("dense-plaintext", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(body)))
		for i := 0; i < b.N; i++ {
			if _, _, err := tarstream.WriteTo(context.Background(), io.Discard, "image", dense); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("dense-encrypted", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(body)))
		for i := 0; i < b.N; i++ {
			if _, _, err := tarstream.WriteTo(context.Background(), io.Discard, "image", dense, tarstream.WithCodec(codec, true)); err != nil {
				b.Fatal(err)
			}
		}
	})

	const logicalSize = uint64(1 << 30)
	const dataSize = uint64(4 << 20)
	sparseSource, err := sparse.NewSource(
		fillReaderAt(0x7c),
		logicalSize,
		[]sparse.Extent{{Offset: dataSize, Size: logicalSize - 2*dataSize}},
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("sparse-1GiB-logical-encrypted", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(2 * dataSize))
		for i := 0; i < b.N; i++ {
			if _, _, err := tarstream.WriteTo(context.Background(), io.Discard, "image", sparseSource, tarstream.WithCodec(codec, true)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type fillReaderAt byte

func (value fillReaderAt) ReadAt(buffer []byte, _ int64) (int, error) {
	for i := range buffer {
		buffer[i] = byte(value)
	}
	return len(buffer), nil
}

func consumeData(source sparse.Source) error {
	buffer := make([]byte, 256<<10)
	for offset := uint64(0); offset < source.Size(); {
		run, err := source.RunAt(offset, source.Size()-offset)
		if err != nil {
			return err
		}
		if run.Kind() == sparse.Hole {
			offset = run.End()
			continue
		}
		for offset < run.End() {
			length := min(uint64(len(buffer)), run.End()-offset)
			n, err := run.ReadAt(context.Background(), buffer[:length], offset-run.Offset())
			offset += uint64(n)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrNoProgress
			}
		}
	}
	return nil
}
