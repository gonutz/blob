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

// New creates an empty blob. You can add resources to it using Append. After
// adding all resources, you can call Write to write it to a file for example.
func New() *Blob {
	return &Blob{}
}

// ItemCount returns the number of blob items. When using GetIDAtIndex or
// GetByIndex, valid inidices range from 0 to ItemCount()-1.
func (b *Blob) ItemCount() int {
	return len(b.header)
}

// Append adds the given data at the end of the blob.
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

// GetByID searches the blob for an entry with the given ID and returns the
// first one found. If there is no entry with the given ID, data will be nil and
// found will be false.
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

// GetIDAtIndex returns the ID of the entry at index i or the empty string if
// the given index is out of bounds. See ItemCount for the number of items.
func (b *Blob) GetIDAtIndex(i int) string {
	if i < 0 || i >= len(b.header) {
		return ""
	}
	return b.header[i].id
}

// GetByIndex returns the data of the entry at index i. If the index is out of
// bounds, data will be nil and found will be false. See ItemCount for the
// number of items.
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
// Format (all numbers are encoded in little endian byte order):
// - uint32: Header length in bytes, of the header starting after this number
// - header starts here, it consists of mulitple entries structured as follows:
// - uint16 ID length in bytes, length of the following ID
// - string ID, UTF-8 encoded
// - uint64 data length in bytes, of the data associated with this ID
// - header ends here, the binary data starts directly after the header
// - all data byte slices are simply appended and written as one blob after the
// header
//
// Note that the header does not store offsets into the data explicitly, it
// only stores the length of each item so the offset can be computed from the
// cumulative sum of all data lengths of items that come before it.
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

// Read reads a binary blob from the given reader. If an error occurs, the
// returned blob will be nil. See Write for a description of the data format.
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
	headerReader := bytes.NewBuffer(header)
	for headerReader.Len() > 0 {
		err = binary.Read(headerReader, byteOrder, &idLength)
		if err != nil {
			err = errors.New("reading blob header id length: " + err.Error())
			return
		}

		id := string(headerReader.Next(int(idLength)))
		if len(id) != int(idLength) {
			err = errors.New("reading blob header id: unexpected EOF")
			return
		}

		err = binary.Read(headerReader, byteOrder, &dataLength)
		if err != nil {
			err = errors.New("reading blob header data length: " + err.Error())
			return
		}

		b.header = append(b.header, indexItem{
			id,
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
