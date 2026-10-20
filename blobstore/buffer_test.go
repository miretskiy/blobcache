package blobstore

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func makeBufferChunks(sizes ...int) [][]byte {
	chunks := make([][]byte, len(sizes))
	for i, size := range sizes {
		chunks[i] = make([]byte, size)
	}
	return chunks
}

func TestBufferReader(t *testing.T) {
	for _, tc := range []struct {
		name  string
		text  string
		sizes []int
	}{
		{"empty", "", nil},
		{"one chunk", "hello world!", []int{12}},
		{"uneven chunks", "hello world!", []int{3, 3, 6}},
		{"split UTF-8", "héllo 世界!\n", []int{2, 1, 5, 1, 16}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuffer(makeBufferChunks(tc.sizes...))
			n, err := bytes.NewBufferString(tc.text).WriteTo(b)
			require.NoError(t, err)
			require.Equal(t, int64(len(tc.text)), n)
			require.NoError(t, iotest.TestReader(b, []byte(tc.text)))
			nRead, err := b.Read(nil)
			require.NoError(t, err)
			require.Zero(t, nRead)
		})
	}
}

func TestBufferWriter(t *testing.T) {
	chunks := makeBufferChunks(3, 3, 6)
	b := NewBuffer(chunks)
	var reference bytes.Buffer
	w := io.MultiWriter(b, &reference)
	for _, part := range []string{"h", "ello ", "", "worl", "d!"} {
		n, err := io.WriteString(w, part)
		require.NoError(t, err)
		require.Equal(t, len(part), n)
	}
	require.Equal(t, [][]byte{[]byte("hel"), []byte("lo "), []byte("world!")}, chunks)
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, reference.Bytes(), got)
}

func TestBufferReadFull(t *testing.T) {
	b := NewBuffer(makeBufferChunks(3, 3, 6))
	_, err := io.WriteString(b, "hello world!")
	require.NoError(t, err)
	reference := bytes.NewBufferString("hello world!")
	// Cross chunks, finish with a short read, then read again at EOF.
	for _, size := range []int{5, 10, 1} {
		got, want := make([]byte, size), make([]byte, size)
		n, err := io.ReadFull(b, got)
		wantN, wantErr := io.ReadFull(reference, want)
		require.Equal(t, wantN, n)
		require.Equal(t, wantErr, err)
		require.Equal(t, want[:wantN], got[:n])
	}
}

func TestBufferReadFrom(t *testing.T) {
	const text = "hello 世界\nsecond line\r\nlast line"
	failure := errors.New("source failed")
	for _, tc := range []struct {
		name string
		wrap func(io.Reader) io.Reader
	}{
		{"ordinary reads", func(r io.Reader) io.Reader { return r }},
		{"one byte reads", iotest.OneByteReader},
		{"short reads", iotest.HalfReader},
		{"data with EOF", iotest.DataErrReader},
		{"data with error", func(r io.Reader) io.Reader {
			return iotest.DataErrReader(io.MultiReader(r, iotest.ErrReader(failure)))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuffer(makeBufferChunks(3, 2, 1, 17, 64))
			n, err := b.ReadFrom(tc.wrap(bytes.NewBufferString(text)))
			var reference bytes.Buffer
			wantN, wantErr := reference.ReadFrom(tc.wrap(bytes.NewBufferString(text)))
			require.Equal(t, wantN, n)
			require.Equal(t, wantErr, err)
			got, err := io.ReadAll(b)
			require.NoError(t, err)
			require.Equal(t, reference.Bytes(), got)
		})
	}
}

func TestBufferBufferedText(t *testing.T) {
	const text = "hello 世界\r\n\nlast line without newline"
	// Split both a UTF-8 character and CRLF across chunk boundaries.
	b := NewBuffer(makeBufferChunks(7, 1, 5, 1, 1, 4, len(text)))
	w := bufio.NewWriterSize(b, 5)
	n, err := io.Copy(w, bytes.NewBufferString(text))
	require.NoError(t, err)
	require.Equal(t, int64(len(text)), n)
	require.NoError(t, w.Flush())
	r := bufio.NewReaderSize(b, 16)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "hello 世界\r\n", line)
	scanner := bufio.NewScanner(r)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, []string{"", "last line without newline"}, lines)
}

func TestBufferCopyPipeAndFile(t *testing.T) {
	const text = "hello world!\nwritten through a pipe and a file\n"
	b := NewBuffer(makeBufferChunks(3, 3, 6, 1, 64))
	r, w := io.Pipe()
	defer r.Close()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(w, bytes.NewBufferString(text))
		w.CloseWithError(err)
		done <- err
	}()
	// PipeReader has only Read; io.Copy calls our ReadFrom.
	n, err := io.Copy(b, r)
	require.NoError(t, err)
	require.Equal(t, int64(len(text)), n)
	require.NoError(t, <-done)

	file, err := os.CreateTemp(t.TempDir(), "buffer")
	require.NoError(t, err)
	defer file.Close()
	// io.Copy calls our WriteTo, even though File also has ReadFrom.
	n, err = io.Copy(file, b)
	require.NoError(t, err)
	require.Equal(t, int64(len(text)), n)
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	b.Reset()
	n, err = b.ReadFrom(file)
	require.NoError(t, err)
	require.Equal(t, int64(len(text)), n)
	var got bytes.Buffer
	n, err = io.Copy(&got, b)
	require.NoError(t, err)
	require.Equal(t, int64(len(text)), n)
	require.Equal(t, text, got.String())
}

type bufferTestWriter func([]byte) (int, error)

func (f bufferTestWriter) Write(p []byte) (int, error) { return f(p) }

func TestBufferWriteToPartialDestination(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"destination error", io.ErrClosedPipe, io.ErrClosedPipe},
		{"short write without error", nil, io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuffer(makeBufferChunks(3, 3, 6))
			_, err := io.WriteString(b, "hello world!")
			require.NoError(t, err)
			var got bytes.Buffer
			w := bufferTestWriter(func(p []byte) (int, error) {
				n, _ := got.Write(p[:min(len(p), 5-got.Len())])
				if n < len(p) {
					return n, tc.err
				}
				return n, nil
			})
			n, err := b.WriteTo(w)
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, int64(5), n)
			require.Equal(t, "hello", got.String())
			n, err = b.WriteTo(&got)
			require.NoError(t, err)
			require.Equal(t, int64(7), n)
			require.Equal(t, "hello world!", got.String())
		})
	}
}

func TestBufferFixedCapacity(t *testing.T) {
	b := NewBuffer(makeBufferChunks(3, 3, 6))
	n, err := io.WriteString(b, "hello world!extra")
	require.ErrorIs(t, err, ErrBufferFull)
	require.Equal(t, 12, n)
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, "hello world!", string(got))
	require.Equal(t, int64(12), b.Cap())
	require.Zero(t, b.Available(), "reads do not release memory for overwriting")

	b.Reset()
	source := bytes.NewBufferString("hello world!extra")
	n64, err := b.ReadFrom(source)
	require.ErrorIs(t, err, ErrBufferFull)
	require.Equal(t, int64(12), n64)
	var result bytes.Buffer
	_, err = b.WriteTo(&result)
	require.NoError(t, err)
	b.Reset()
	n64, err = b.ReadFrom(source)
	require.NoError(t, err)
	require.Equal(t, int64(5), n64)
	_, err = b.WriteTo(&result)
	require.NoError(t, err)
	require.Equal(t, "hello world!extra", result.String())
}
