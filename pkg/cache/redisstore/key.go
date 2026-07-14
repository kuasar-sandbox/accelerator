package redisstore

import (
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const (
	keyVersion byte = 1
	keySize         = 35

	kindObject byte = 1
	kindShard  byte = 2
)

func encodeKey(p store.Partition, kind byte, contentKey store.ContentKey) ([keySize]byte, error) {
	var key [keySize]byte
	key[0] = keyVersion
	switch p {
	case store.PartitionChunk:
		key[1] = 1
	case store.PartitionManifest:
		key[1] = 2
	case store.PartitionBlob:
		key[1] = 3
	default:
		return key, fmt.Errorf("redisstore: unknown partition %q", p)
	}
	if kind != kindObject && kind != kindShard {
		return key, fmt.Errorf("redisstore: unknown key kind %d", kind)
	}
	key[2] = kind
	copy(key[3:], contentKey[:])
	return key, nil
}
