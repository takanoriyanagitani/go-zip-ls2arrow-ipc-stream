package zipls2ipc

import (
	"archive/zip"
	"database/sql"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	fieldIndexName = iota
	fieldIndexCompressedSize
	fieldIndexUncompressedSize
	fieldIndexModified
	fieldIndexCRC32
	fieldIndexMethod
	fieldIndexZipFilename
)

func TestZipReaderToBatch(t *testing.T) {
	// Create a mock zip.Reader with some in-memory zip.File objects
	files := []*zip.File{
		{
			FileHeader: zip.FileHeader{
				Name:               "file1.txt",
				CompressedSize64:   100,
				UncompressedSize64: 200,
				Modified:           time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				CRC32:              12345,
				Method:             zip.Deflate,
			},
		},
		{
			FileHeader: zip.FileHeader{
				Name:               "file2.txt",
				CompressedSize64:   300,
				UncompressedSize64: 400,
				Modified:           time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC),
				CRC32:              67890,
				Method:             zip.Store,
			},
		},
	}

	mockZipReader := &zip.Reader{File: files}

	reader := ZipReader{mockZipReader}

	// Test with a valid zip filename
	zipFilename := sql.Null[string]{V: "test.zip", Valid: true}
	batch, err := reader.ToBatch(zipFilename)
	if err != nil {
		t.Fatalf("ToBatch failed: %v", err)
	}
	defer batch.Release()

	// Assert schema
	if !batch.Schema().Equal(ZipItemSchema) {
		t.Errorf("Schema mismatch: got %v, want %v", batch.Schema(), ZipItemSchema)
	}

	// Assert number of rows
	if batch.NumRows() != int64(len(files)) {
		t.Errorf("NumRows mismatch: got %d, want %d", batch.NumRows(), len(files))
	}

	// Assert data for each column
	assertStringColumn(t, batch, fieldIndexName, []string{"file1.txt", "file2.txt"})
	assertUint64Column(t, batch, fieldIndexCompressedSize, []uint64{100, 300})
	assertUint64Column(t, batch, fieldIndexUncompressedSize, []uint64{200, 400})
	assertTimestampColumn(t, batch, fieldIndexModified, []time.Time{
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC),
	})
	assertUint32Column(t, batch, fieldIndexCRC32, []uint32{12345, 67890})
	assertUint16Column(t, batch, fieldIndexMethod, []uint16{zip.Deflate, zip.Store})
	assertStringColumn(t, batch, fieldIndexZipFilename, []string{"test.zip", "test.zip"})

	// Test with a null zip filename
	zipFilename = sql.Null[string]{Valid: false}
	batch, err = reader.ToBatch(zipFilename)
	if err != nil {
		t.Fatalf("ToBatch failed with null zip filename: %v", err)
	}
	defer batch.Release()

	assertNullStringColumn(t, batch, fieldIndexZipFilename, len(files))
}

func assertStringColumn(t *testing.T, batch arrow.RecordBatch, colIdx int, expected []string) {
	col := batch.Column(colIdx).(*array.String)
	for i, v := range expected {
		if col.Value(i) != v {
			t.Errorf("Column %d, row %d: got %s, want %s", colIdx, i, col.Value(i), v)
		}
	}
}

func assertUint64Column(t *testing.T, batch arrow.RecordBatch, colIdx int, expected []uint64) {
	col := batch.Column(colIdx).(*array.Uint64)
	for i, v := range expected {
		if col.Value(i) != v {
			t.Errorf("Column %d, row %d: got %d, want %d", colIdx, i, col.Value(i), v)
		}
	}
}

func assertTimestampColumn(t *testing.T, batch arrow.RecordBatch, colIdx int, expected []time.Time) {
	col := batch.Column(colIdx).(*array.Timestamp)

	dt := batch.Schema().Field(colIdx).Type
	timestampType, ok := dt.(*arrow.TimestampType)
	if !ok {
		t.Fatalf("Column %d is not a TimestampType", colIdx)
	}

	for i, v := range expected {
		if !col.Value(i).ToTime(timestampType.Unit).Equal(v) {
			t.Errorf("Column %d, row %d: got %v, want %v", colIdx, i, col.Value(i).ToTime(timestampType.Unit), v)
		}
	}
}

func assertUint32Column(t *testing.T, batch arrow.RecordBatch, colIdx int, expected []uint32) {
	col := batch.Column(colIdx).(*array.Uint32)
	for i, v := range expected {
		if col.Value(i) != v {
			t.Errorf("Column %d, row %d: got %d, want %d", colIdx, i, col.Value(i), v)
		}
	}
}

func assertUint16Column(t *testing.T, batch arrow.RecordBatch, colIdx int, expected []uint16) {
	col := batch.Column(colIdx).(*array.Uint16)
	for i, v := range expected {
		if col.Value(i) != v {
			t.Errorf("Column %d, row %d: got %d, want %d", colIdx, i, col.Value(i), v)
		}
	}
}

func assertNullStringColumn(t *testing.T, batch arrow.RecordBatch, colIdx int, numRows int) {
	col := batch.Column(colIdx).(*array.String)
	for i := 0; i < numRows; i++ {
		if col.IsValid(i) {
			t.Errorf("Column %d, row %d: got valid value, want null", colIdx, i)
		}
	}
}
