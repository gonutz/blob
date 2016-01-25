package blob_test

import (
	"bytes"
	"github.com/gonutz/blob"
	"testing"
)

func TestEmptyBlobJustWritesZeroHeaderLength(t *testing.T) {
	b := blob.New()
	buffer := bytes.NewBuffer(nil)

	err := b.Write(buffer)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buffer.Bytes(), []byte{0, 0, 0, 0})
}

func TestOneResourceMakesOneHeaderEntry(t *testing.T) {
	b := blob.New()
	buffer := bytes.NewBuffer(nil)

	b.Append("id", []byte{1, 2, 3})
	err := b.Write(buffer)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buffer.Bytes(), []byte{
		12, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		byte('i'), byte('d'),
		3, 0, 0, 0, 0, 0, 0, 0, // data length
		1, 2, 3, // actual data
	})
}

func TestTwoResourcesMakeTwoEntries(t *testing.T) {
	b := blob.New()
	buffer := bytes.NewBuffer(nil)

	b.Append("id", []byte{1, 2, 3})
	b.Append("2nd", []byte{4, 5})
	err := b.Write(buffer)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buffer.Bytes(), []byte{
		25, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		byte('i'), byte('d'),
		3, 0, 0, 0, 0, 0, 0, 0, // data length
		3, 0, // "2nd" is 3 bytes long
		byte('2'), byte('n'), byte('d'),
		2, 0, 0, 0, 0, 0, 0, 0, // data length
		1, 2, 3, // data for "id"
		4, 5, // data for "2nd"
	})
}

func TestZeroLengthEntryIsStillRepresentedInHeader(t *testing.T) {
	b := blob.New()
	buffer := bytes.NewBuffer(nil)

	b.Append("id", []byte{})
	err := b.Write(buffer)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buffer.Bytes(), []byte{
		12, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		byte('i'), byte('d'),
		0, 0, 0, 0, 0, 0, 0, 0, // data length
		// there is no data, the slice is empty
	})
}

func TestZeroLengthEntryCanGoBetweenTwoEntries(t *testing.T) {
	b := blob.New()
	buffer := bytes.NewBuffer(nil)

	b.Append("1", []byte{1})
	b.Append("_", []byte{})
	b.Append("2", []byte{2})
	err := b.Write(buffer)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buffer.Bytes(), []byte{
		33, 0, 0, 0,
		1, 0, // "1" is 1 byte long
		byte('1'),
		1, 0, 0, 0, 0, 0, 0, 0, // data length
		1, 0, // "_" is 1 byte long
		byte('_'),
		0, 0, 0, 0, 0, 0, 0, 0, // data length
		1, 0, // "2" is 1 byte long
		byte('2'),
		1, 0, 0, 0, 0, 0, 0, 0, // data length
		1, 2, // data
	})
}

func TestReadingEmptyBlobReturnsZeroItems(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{
		0, 0, 0, 0, // empty header, 0 length
	})

	b, err := blob.Read(buffer)

	if err != nil {
		t.Fatal(err)
	}
	if b.ItemCount() != 0 {
		t.Fatal("item count was", b.ItemCount())
	}
}

func TestReadingOneEntryBlob(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{
		12, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		byte('i'), byte('d'),
		3, 0, 0, 0, 0, 0, 0, 0, // data length
		1, 2, 3, // actual data
	})

	b, err := blob.Read(buffer)

	if err != nil {
		t.Fatal(err)
	}
	if b.ItemCount() != 1 {
		t.Fatal("item count was", b.ItemCount())
	}
	// item 1
	data, found := b.GetByID("id")
	if !found {
		t.Fatal("id not found")
	}
	checkBytes(t, data, []byte{1, 2, 3})
}

func TestReadingTwoEntryBlob(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{
		25, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		byte('i'), byte('d'),
		3, 0, 0, 0, 0, 0, 0, 0, // data length
		3, 0, // "2nd" is 3 bytes long
		byte('2'), byte('n'), byte('d'),
		2, 0, 0, 0, 0, 0, 0, 0, // data length
		1, 2, 3, // data for "id"
		4, 5, // data for "2nd"
	})

	b, err := blob.Read(buffer)

	if err != nil {
		t.Fatal(err)
	}
	if b.ItemCount() != 2 {
		t.Fatal("item count was", b.ItemCount())
	}
	// item 1
	data, found := b.GetByID("id")
	if !found {
		t.Fatal("id not found")
	}
	checkBytes(t, data, []byte{1, 2, 3})
	// item 2
	data, found = b.GetByID("2nd")
	if !found {
		t.Fatal("2nd not found")
	}
	checkBytes(t, data, []byte{4, 5})
}

func TestReadingZeroLengthDataEntry(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{
		12, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		byte('i'), byte('d'),
		0, 0, 0, 0, 0, 0, 0, 0, // data length
		// no data, length is 0
	})

	b, err := blob.Read(buffer)

	if err != nil {
		t.Fatal(err)
	}
	if b.ItemCount() != 1 {
		t.Fatal("item count was", b.ItemCount())
	}
	// item 1
	data, found := b.GetByID("id")
	if !found {
		t.Fatal("id not found")
	}
	checkBytes(t, data, []byte{})
}

func TestAccessFunctions(t *testing.T) {
	b := blob.New()
	b.Append("one", []byte{1, 2, 3})
	b.Append("two", []byte{4, 5})

	if b.ItemCount() != 2 {
		t.Error("item count was", b.ItemCount())
	}

	one, found := b.GetByID("one")
	if !found {
		t.Error("one not found")
	}
	checkBytes(t, one, []byte{1, 2, 3})

	two, found := b.GetByIndex(1)
	if !found {
		t.Error("two not found by index")
	}
	checkBytes(t, two, []byte{4, 5})

	if id := b.GetIDAtIndex(0); id != "one" {
		t.Error("expected id is one but was", id)
	}
	if id := b.GetIDAtIndex(1); id != "two" {
		t.Error("expected id is two but was", id)
	}
}

func checkBytes(t *testing.T, got, want []byte) {
	if len(got) != len(want) {
		t.Fatalf("different lengths, want %v, but got %v", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at byte %v wanted %v but got %v", i, want[i], got[i])
		}
	}
}
