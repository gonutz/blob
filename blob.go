package blob

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

type Blob struct {
	header []indexItem
	data   []byte
}

type indexItem struct {
	id    string
	start uint64
	end   uint64
}

// New creates an empty blob that you can add your resuorces to and later save
// it to a file.
func New() *Blob {
	return &Blob{}
}

func (b *Blob) ItemCount() int {
	return len(b.header)
}

func (b *Blob) Append(id string, data []byte) {
	b.header = append(
		b.header,
		indexItem{
			id,
			uint64(len(b.data)),
			uint64(len(b.data) + len(data)),
		},
	)
	b.data = append(b.data, data...)
}

func (b *Blob) GetByID(id string) (data []byte, found bool) {
	for i := range b.header {
		if b.header[i].id == id {
			data = b.data[b.header[i].start:b.header[i].end]
			found = true
			return
		}
	}
	return
}

func (b *Blob) GetByIndex(i int) (data []byte, found bool) {
	if i < 0 || i >= len(b.header) {
		return
	}
	data = b.data[b.header[i].start:b.header[i].end]
	found = true
	return
}

// Write writes the whole binary blob to the given writer.
//
// Format: the data is structured as follows, numbers are encoded in little
// endian byte order:
// 1. Header length in bytes uint32, this is the overall length off the header,
//    starting after this uint32
// 2. Header: consists of consecutive items, each of which are structured as
//    follows:
//  2.1. ID length in bytes, uint16 giving the length of the following ID string
//  2.2. ID, this is a string
//  2.3. Data length, this uint64 gives the length of the data for this item in
//       bytes
// 3. Data, it starts directly after the header so the offset into the overall
// file is the header lenght plus 4 bytes for the header header length itselfe,
// which is a uint32.
// For each item only the length is stored, the offset into the data can be
// computed by summing up the lengths of the items coming before that.
func (b *Blob) Write(w io.Writer) (err error) {
	buffer := bytes.NewBuffer(nil)
	for i := range b.header {
		// first write the ID length and then the ID
		err = binary.Write(buffer, byteOrder, uint16(len(b.header[i].id)))
		if err != nil {
			err = errors.New("writing blob header id length: " + err.Error())
			return
		}
		_, err = buffer.Write([]byte(b.header[i].id))
		if err != nil {
			err = errors.New("writing blob header id: " + err.Error())
			return
		}

		length := b.header[i].end - b.header[i].start
		err = binary.Write(buffer, byteOrder, length)
		if err != nil {
			err = errors.New("writing blob header data length: " + err.Error())
			return
		}
	}
	// write the header length
	err = binary.Write(w, byteOrder, uint32(buffer.Len()))
	if err != nil {
		err = errors.New("writing blob header length: " + err.Error())
		return
	}
	// write the actual header data
	_, err = w.Write(buffer.Bytes())
	if err != nil {
		err = errors.New("writing blob header: " + err.Error())
		return
	}
	// write the data
	_, err = w.Write(b.data)
	if err != nil {
		err = errors.New("writing blob data: " + err.Error())
		return
	}
	return nil
}

var byteOrder = binary.LittleEndian

// Read reads a binary blob from the given reader. If a read fails it returns
// that read's error. If the error is non-nil the returned blob is nil.
// See Blob.Write for a description of the data format.
func Read(r io.Reader) (blob *Blob, err error) {
	var b Blob

	// read header length
	var headerLength uint32
	err = binary.Read(r, byteOrder, &headerLength)
	if err != nil {
		err = errors.New("reading blob header length: " + err.Error())
		return
	}

	// read the actual header
	header := make([]byte, headerLength)
	_, err = r.Read(header)
	if err != nil {
		err = errors.New("reading blob header: " + err.Error())
		return
	}

	// dissect the header, keeping track of the overall data length
	var overallDataLength uint64
	var dataLength uint64
	var idLength uint16
	headerReader := bytes.NewReader(header)
	for headerReader.Len() > 0 {
		err = binary.Read(headerReader, byteOrder, &idLength)
		if err != nil {
			err = errors.New("reading blob header id length: " + err.Error())
			return
		}

		id := make([]byte, idLength)
		_, err = io.ReadFull(headerReader, id)
		if err != nil {
			err = errors.New("reading blob header id: " + err.Error())
			return
		}

		err = binary.Read(headerReader, byteOrder, &dataLength)
		if err != nil {
			err = errors.New("reading blob header data length: " + err.Error())
			return
		}

		b.header = append(b.header, indexItem{
			string(id),
			overallDataLength,
			overallDataLength + dataLength,
		})

		overallDataLength += dataLength
	}

	if overallDataLength > 0 {
		b.data = make([]byte, overallDataLength)
		_, err = io.ReadFull(r, b.data)
		if err != nil {
			err = errors.New("reading blob data: " + err.Error())
			return
		}
	}

	blob = &b
	return
}
