package zipls2ipc

import (
	"archive/zip"
	"bufio"
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	ai "github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

type IpcStreamWriter struct{ *ai.Writer }

func (w IpcStreamWriter) Close() error { return w.Writer.Close() }

func (w IpcStreamWriter) WriteBatch(b arrow.RecordBatch) error {
	return w.Writer.Write(b)
}

func ZipInfoIpcWriterNew(wtr io.Writer, opts ...ai.Option) IpcStreamWriter {
	return IpcStreamWriter{
		Writer: ai.NewWriter(wtr, opts...),
	}
}

// ZipReader is a wrapper around zip.Reader that provides additional functionality.
type ZipReader struct{ *zip.Reader }

func (r ZipReader) Files() []*zip.File { return r.Reader.File }

type ZipItem struct{ *zip.File }

func (i ZipItem) Name() string               { return i.File.Name }
func (i ZipItem) CompressedSize64() uint64   { return i.File.CompressedSize64 }
func (i ZipItem) UncompressedSize64() uint64 { return i.File.UncompressedSize64 }
func (i ZipItem) Modified() time.Time        { return i.File.Modified }
func (i ZipItem) CRC32() uint32              { return i.File.CRC32 }
func (i ZipItem) Method() uint16             { return i.File.Method }

var ZipItemSchema = arrow.NewSchema(
	[]arrow.Field{
		{
			Name:     "name",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
			Metadata: arrow.MetadataFrom(make(map[string]string)),
		},
		{
			Name:     "compressed_size",
			Type:     arrow.PrimitiveTypes.Uint64,
			Nullable: false,
			Metadata: arrow.MetadataFrom(make(map[string]string)),
		},
		{
			Name:     "uncompressed_size",
			Type:     arrow.PrimitiveTypes.Uint64,
			Nullable: false,
			Metadata: arrow.MetadataFrom(make(map[string]string)),
		},
		{
			Name:     "modified",
			Type:     arrow.FixedWidthTypes.Timestamp_ms,
			Nullable: false,
			Metadata: arrow.MetadataFrom(make(map[string]string)),
		},
		{
			Name:     "crc32",
			Type:     arrow.PrimitiveTypes.Uint32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(make(map[string]string)),
		},
		{
			Name:     "method",
			Type:     arrow.PrimitiveTypes.Uint16,
			Nullable: false,
			Metadata: arrow.MetadataFrom(make(map[string]string)),
		},
		{
			Name:     "zip_filename",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
			Metadata: arrow.MetadataFrom(make(map[string]string)),
		},
	},
	nil,
)

var (
	ErrUnexpectedTimestampType      = errors.New("unexpected timestamp type")
	ErrFileDoesNotImplementReaderAt = errors.New("file does not implement io.ReaderAt")
)

// newZipItemArrays creates a slice of arrow.Array from a zip.Reader.
// The caller is responsible for releasing the returned arrays.
func newZipItemArrays(reader *zip.Reader, mem memory.Allocator) ([]arrow.Array, error) {
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()

	compressedSizeBuilder := array.NewUint64Builder(mem)
	defer compressedSizeBuilder.Release()

	uncompressedSizeBuilder := array.NewUint64Builder(mem)
	defer uncompressedSizeBuilder.Release()

	timestampType, ok := arrow.FixedWidthTypes.Timestamp_ms.(*arrow.TimestampType)
	if !ok {
		return nil, ErrUnexpectedTimestampType
	}
	modifiedBuilder := array.NewTimestampBuilder(mem, timestampType)
	defer modifiedBuilder.Release()

	crc32Builder := array.NewUint32Builder(mem)
	defer crc32Builder.Release()

	methodBuilder := array.NewUint16Builder(mem)
	defer methodBuilder.Release()

	for _, f := range reader.File {
		item := ZipItem{f}
		nameBuilder.Append(item.Name())
		compressedSizeBuilder.Append(item.CompressedSize64())
		uncompressedSizeBuilder.Append(item.UncompressedSize64())
		modifiedBuilder.Append(arrow.Timestamp(item.Modified().UnixMilli()))
		crc32Builder.Append(item.CRC32())
		methodBuilder.Append(item.Method())
	}

	nameArray := nameBuilder.NewArray()
	compressedSizeArray := compressedSizeBuilder.NewArray()
	uncompressedSizeArray := uncompressedSizeBuilder.NewArray()
	modifiedArray := modifiedBuilder.NewArray()
	crc32Array := crc32Builder.NewArray()
	methodArray := methodBuilder.NewArray()
	return []arrow.Array{
		nameArray,
		compressedSizeArray,
		uncompressedSizeArray,
		modifiedArray,
		crc32Array,
		methodArray,
	}, nil
}

// ToBatch converts the zip file's metadata into an arrow.RecordBatch.
// The caller is responsible for releasing the returned RecordBatch.
func (r ZipReader) ToBatch(zipFilename sql.Null[string]) (arrow.RecordBatch, error) {
	mem := memory.DefaultAllocator

	arrays, err := newZipItemArrays(r.Reader, mem)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, arr := range arrays {
			arr.Release()
		}
	}()

	var zipFilenameArray arrow.Array
	if zipFilename.Valid {
		zipFilenameScalar := scalar.NewStringScalar(zipFilename.V)
		zipFilenameArray, err = scalar.MakeArrayFromScalar(zipFilenameScalar, len(r.Files()), mem)
	} else {
		nullScalar := scalar.MakeNullScalar(arrow.BinaryTypes.String)
		zipFilenameArray, err = scalar.MakeArrayFromScalar(nullScalar, len(r.Files()), mem)
	}
	if err != nil {
		return nil, err
	}

	arrays = append(arrays, zipFilenameArray)

	return array.NewRecord(ZipItemSchema, arrays, int64(len(r.Files()))), nil
}

type ZipFileLike struct {
	io.ReaderAt

	Size int64
}

func (l ZipFileLike) ToReader() (ZipReader, error) {
	rdr, e := zip.NewReader(l.ReaderAt, l.Size)
	return ZipReader{Reader: rdr}, e
}

func (l ZipFileLike) ToBatch(zipFilename sql.Null[string]) (arrow.RecordBatch, error) {
	rdr, e := l.ToReader()
	if nil != e {
		return nil, e
	}

	return rdr.ToBatch(zipFilename)
}

type ZipFile struct{ fs.File }

func (f ZipFile) Close() error { return f.File.Close() }

func (f ZipFile) ToReader() (ZipReader, error) {
	rat, ok := f.File.(io.ReaderAt)
	if !ok {
		return ZipReader{}, ErrFileDoesNotImplementReaderAt
	}

	finfo, e := f.File.Stat()
	if nil != e {
		return ZipReader{}, e
	}

	return ZipFileLike{
		ReaderAt: rat,
		Size:     finfo.Size(),
	}.ToReader()
}

func (f ZipFile) ToBatch(zipFilename sql.Null[string]) (arrow.RecordBatch, error) {
	rdr, e := f.ToReader()
	if nil != e {
		return nil, e
	}

	return rdr.ToBatch(zipFilename)
}

type ZipFilename string

func (n ZipFilename) ToZipFile() (ZipFile, error) {
	f, e := os.Open(string(n))
	return ZipFile{File: f}, e
}

func (n ZipFilename) ToBatch() (arrow.RecordBatch, error) {
	zfile, err := n.ToZipFile()
	if err != nil {
		return nil, err
	}
	defer zfile.Close() //nolint:errcheck

	rdr, e := zfile.ToReader()
	if nil != e {
		return nil, e
	}

	oname := sql.Null[string]{
		V:     string(n),
		Valid: true,
	}
	return rdr.ToBatch(oname)
}

func (n ZipFilename) ToIpcStreamWriter(writer IpcStreamWriter) error {
	batch, err := n.ToBatch()
	if err != nil {
		return err
	}
	defer batch.Release()

	return writer.WriteBatch(batch)
}

func (n ZipFilename) ToWriter(wtr io.Writer) error {
	w := ZipInfoIpcWriterNew(wtr, ai.WithSchema(ZipItemSchema))
	return errors.Join(n.ToIpcStreamWriter(w), w.Close())
}

func (n ZipFilename) ToStdout() error {
	var bw *bufio.Writer = bufio.NewWriter(os.Stdout)
	return errors.Join(n.ToWriter(os.Stdout), bw.Flush())
}
