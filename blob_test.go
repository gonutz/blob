package blob_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/gonutz/blob"
)

func TestEmptyBlobJustWritesZeroHeaderLength(t *testing.T) {
	b := blob.New()
	var buf bytes.Buffer

	err := b.Write(&buf)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buf.Bytes(), []byte{0, 0, 0, 0})
}

func TestOneResourceMakesOneHeaderEntry(t *testing.T) {
	b := blob.New()
	var buf bytes.Buffer

	b.Append("id", []byte{1, 2, 3})
	err := b.Write(&buf)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buf.Bytes(), []byte{
		12, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		'i', 'd',
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
		'i', 'd',
		3, 0, 0, 0, 0, 0, 0, 0, // data length for "id" data
		3, 0, // "2nd" is 3 bytes long
		'2', 'n', 'd',
		2, 0, 0, 0, 0, 0, 0, 0, // data length for "2nd" data
		1, 2, 3, // data for "id"
		4, 5, // data for "2nd"
	})
}

func TestZeroLengthEntryIsStillRepresentedInHeader(t *testing.T) {
	b := blob.New()
	var buf bytes.Buffer

	b.Append("id", []byte{})
	err := b.Write(&buf)

	if err != nil {
		t.Fatal(err)
	}
	checkBytes(t, buf.Bytes(), []byte{
		12, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		'i', 'd',
		0, 0, 0, 0, 0, 0, 0, 0, // data length
		// there is no data, "id" data is empty
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
		'1',
		1, 0, 0, 0, 0, 0, 0, 0, // data length for "1" data
		1, 0, // "_" is 1 byte long
		'_',
		0, 0, 0, 0, 0, 0, 0, 0, // data length for "_" data
		1, 0, // "2" is 1 byte long
		'2',
		1, 0, 0, 0, 0, 0, 0, 0, // data length for "2" data
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
		'i', 'd',
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
		'i', 'd',
		3, 0, 0, 0, 0, 0, 0, 0, // data length
		3, 0, // "2nd" is 3 bytes long
		'2', 'n', 'd',
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
		'i', 'd',
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

func TestWritingMaxLengthIDIsFine(t *testing.T) {
	b := blob.New()
	var id [blob.MaxIDLen]byte
	for i := range id {
		id[i] = 'a'
	}
	b.Append(string(id[:]), nil)
	var buf bytes.Buffer

	err := b.Write(&buf)

	if err != nil {
		t.Error(err)
	}
}

func TestWritingFailsIfIDIsTooLong(t *testing.T) {
	b := blob.New()
	var id [blob.MaxIDLen + 1]byte
	for i := range id {
		id[i] = 'a'
	}
	b.Append(string(id[:]), nil)
	var buf bytes.Buffer

	err := b.Write(&buf)

	if err == nil {
		t.Error("error expected but got", buf.Bytes())
	}
}

func TestOpenBlobAndReadData(t *testing.T) {
	b := blob.New()
	b.Append("one", []byte{1, 2, 3})
	b.Append("two", []byte{4, 5})
	var buf bytes.Buffer
	b.Write(&buf)
	r := bytes.NewReader(buf.Bytes())

	br, err := blob.Open(r)
	if err != nil {
		t.Fatal(err)
	}

	{
		if n := br.ItemCount(); n != 2 {
			t.Error("want 2 items but have", n)
		}
		if s := br.GetIDAtIndex(0); s != "one" {
			t.Error("want 'one' but have", s)
		}
		if s := br.GetIDAtIndex(1); s != "two" {
			t.Error("want 'two' but have", s)
		}
	}

	{
		one, found := br.GetByID("one")
		if !found {
			t.Error("one not found")
		}
		all, err := ioutil.ReadAll(one)
		if err != nil {
			t.Error("reading one", err)
		}
		checkBytes(t, all, []byte{1, 2, 3})
	}

	{
		two, found := br.GetByID("two")
		if !found {
			t.Error("two not found")
		}
		all, err := ioutil.ReadAll(two)
		if err != nil {
			t.Error("reading two", err)
		}
		checkBytes(t, all, []byte{4, 5})
	}

	{
		one, found := br.GetByID("one")
		if !found {
			t.Error("one not found the second time")
		}

		pos, err := one.Seek(2, io.SeekStart)
		if err != nil {
			t.Error(err)
		}
		if pos != 2 {
			t.Error("want pos from start to be 2, got", pos)
		}

		var buffer [32]byte
		n, err := one.Read(buffer[:])
		if err != nil {
			t.Error("reading last one byte", err)
		}
		if n != 1 {
			t.Error("want one last byte of one but have", n)
		}
		if buffer[0] != 3 {
			t.Error("wrong last byte in one:", buffer[:2])
		}

		pos, err = one.Seek(-1, io.SeekEnd)
		if err != nil {
			t.Error(err)
		}
		if pos != 2 {
			t.Error("want pos from end to be 2, got", pos)
		}

		pos, err = one.Seek(-1, io.SeekCurrent)
		if err != nil {
			t.Error(err)
		}
		if pos != 1 {
			t.Error("want pos from current to be 1, got", pos)
		}
		n, err = one.Read(buffer[0:1])
		if err != nil {
			t.Error("reading second one byte", err)
		}
		if n != 1 {
			t.Error("want one last byte of one but have", n)
		}
		if buffer[0] != 2 {
			t.Error("wrong last byte in one:", buffer[:1])
		}
	}
}

func TestOpenEmptyBlob(t *testing.T) {
	buffer := bytes.NewReader([]byte{
		0, 0, 0, 0, // empty header, 0 length
	})

	b, err := blob.Open(buffer)

	if err != nil {
		t.Fatal(err)
	}
	if b.ItemCount() != 0 {
		t.Fatal("item count was", b.ItemCount())
	}
}

func TestOpenBlobWithEmptyItem(t *testing.T) {
	buffer := bytes.NewReader([]byte{
		12, 0, 0, 0,
		2, 0, // "id" is 2 bytes long
		'i', 'd',
		0, 0, 0, 0, 0, 0, 0, 0, // data length
		// no data, length was 0
	})

	b, err := blob.Open(buffer)

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
	all, err := ioutil.ReadAll(data)
	if err != nil {
		t.Error("cannot read data", err)
	}
	checkBytes(t, all, []byte{})
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
