package tarstream

import "testing"

func TestRecordReadBufferPoolSizingAndClear(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		poolIndex int
		capacity  int
	}{
		{name: "empty", size: 0, poolIndex: -1, capacity: 0},
		{name: "minimum", size: 1, poolIndex: 0, capacity: 1 << recordReadBufferMinShift},
		{name: "minimum-boundary", size: 1 << recordReadBufferMinShift, poolIndex: 0, capacity: 1 << recordReadBufferMinShift},
		{name: "next-class", size: (1 << recordReadBufferMinShift) + 1, poolIndex: 1, capacity: 1 << (recordReadBufferMinShift + 1)},
		{name: "power-maximum", size: 1 << recordReadBufferMaxPowerShift, poolIndex: recordReadBufferLargePool - 1, capacity: 1 << recordReadBufferMaxPowerShift},
		{name: "large-class", size: (1 << recordReadBufferMaxPowerShift) + 1, poolIndex: recordReadBufferLargePool, capacity: maxCoalesceBytes + recordAADSize()},
		{name: "large-boundary", size: maxCoalesceBytes + recordAADSize(), poolIndex: recordReadBufferLargePool, capacity: maxCoalesceBytes + recordAADSize()},
		{name: "unpooled", size: maxCoalesceBytes + recordAADSize() + 1, poolIndex: -1, capacity: maxCoalesceBytes + recordAADSize() + 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffer := acquireRecordReadBuffer(test.size)
			if len(buffer.data) != test.size || cap(buffer.data) != test.capacity {
				t.Fatalf("buffer size/capacity = %d/%d, want %d/%d", len(buffer.data), cap(buffer.data), test.size, test.capacity)
			}
			if buffer.poolIndex != test.poolIndex {
				t.Fatalf("pool index = %d, want %d", buffer.poolIndex, test.poolIndex)
			}
			used := buffer.data
			for index := range used {
				used[index] = 0xa5
			}
			releaseRecordReadBuffer(buffer)
			for index, value := range used {
				if value != 0 {
					t.Fatalf("released byte %d = %#x, want zero", index, value)
				}
			}
		})
	}
}
