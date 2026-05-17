package chunker

import "io"

// chunkFixed performs fixed-size chunking. The last chunk may be smaller
// than size. Zero chunks (all bytes zero) have IsZero=true and Data=nil.
func chunkFixed(r io.Reader, size uint32, cb Callback) error {
	buf := make([]byte, size)
	offset := uint64(0)

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := buf[:n]
			isZero := isAllZero(chunk)

			var data []byte
			if !isZero {
				data = make([]byte, n)
				copy(data, chunk)
			}

			if cbErr := cb(ChunkResult{
				Offset: offset,
				Size:   uint32(n),
				Data:   data,
				IsZero: isZero,
			}); cbErr != nil {
				return cbErr
			}

			offset += uint64(n)
		}

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
	}
}
