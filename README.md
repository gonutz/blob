# blob

Create Doom style blob files that combine multiple binary blobs into a single file.

The format is as simple as possible, first there is header that gives each blob a string ID. Then comes the data.

You can create new blobs programmatically (blob.New) and save them to a file (Blob.Write) in a preprocessing step.
Later in your program you can read the file (blob.Read) and access the data by their string ID (Blob.GetByID).

# Documentation

See the [GoDoc for this package](https://godoc.org/github.com/gonutz/blob) for a reference of the API. It is kept minimal for ease of use.