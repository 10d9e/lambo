package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	blocks "github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-car/util"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	ipldprime "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	buildr "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	commp "github.com/jlogelin/lambo/delta"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/urfave/cli/v2"

	carv1 "github.com/ipld/go-car"
)

type MyFile struct {
	*os.File
}

func (mf *MyFile) Write(p []byte) (n int, err error) {
	// Add your own implementation of Write here
	fmt.Println("Writing data to MyFile")
	return mf.File.Write(p)
}

/*
func main() {
	// Open a file for writing
	f, err := os.OpenFile("test.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Wrap the file in a LoggingFile
	lf := LoggingFile{f}

	// Write some data to the file
	lf.Write([]byte("Hello, world!\n"))

	// Write some data to a specific offset in the file
	lf.WriteAt([]byte("Goodbye, world!\n"), 0)

	// Close the file
	lf.Close()
}
*/

// CreateCar creates a car
func CreateCar(c *cli.Context) error {
	var err error
	if c.Args().Len() == 0 {
		return fmt.Errorf("a source location to build the car from must be specified")
	}

	if !c.IsSet("file") {
		return fmt.Errorf("a file destination must be specified")
	}

	// make a cid with the right length that we eventually will patch with the root.
	hasher, err := multihash.GetHasher(multihash.SHA2_256)
	if err != nil {
		return err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, multihash.SHA2_256)
	if err != nil {
		return err
	}
	proxyRoot := cid.NewCidV1(uint64(multicodec.DagPb), hash)

	options := []car.Option{}
	switch c.Int("version") {
	case 1:
		options = []car.Option{blockstore.WriteAsCarV1(true)}
	case 2:
		// already the default
	default:
		return fmt.Errorf("invalid CAR version %d", c.Int("version"))
	}

	// Open a file for writing
	f, err := os.OpenFile(c.String("file"), os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Wrap the file in a MyFile
	//mf := &MyFile{f}

	// Write some data to the file
	//mf.Write([]byte("Hello, world!\n"))

	// Close the file
	//mf.Close()

	cdest, err := blockstore.OpenReadWriteFile(f, []cid.Cid{proxyRoot}, options...)
	// cdest, err := blockstore.OpenReadWrite(c.String("file"), []cid.Cid{proxyRoot}, options...)
	if err != nil {
		return err
	}

	//cp := &commp.Calc{}
	// Write the unixfs blocks into the store.
	root, err := writeFiles(c.Context, cdest, c.Args().Slice()...)
	if err != nil {
		return err
	}

	if err := cdest.Finalize(); err != nil {
		return err
	}

	/*
		nbuf, err := ReplaceRootsInBuffer(buf, []cid.Cid{root})
		if err != nil {
			return err
		}
		cp.Write(nbuf.Bytes())

		defer nbuf.Reset()
		defer buf.Reset()
	*/

	// re-open/finalize with the final root.
	err = car.ReplaceRootsInFile(c.String("file"), []cid.Cid{root})
	if err != nil {
		return err
	}

	/*
		start := time.Now()
		commP, paddedPieceSize, err := cp.Digest()
		if err != nil {
			return err
		}
		p, _ := commcid.PieceCommitmentV1ToCID(commP)
		fmt.Println("generated commP", "duration:", time.Since(start), "commP:", p.String(), "pps:", paddedPieceSize, "mbps:", float64(benchSize)/time.Since(start).Seconds()/1024/1024)

	*/

	post(c.String("file"))

	return nil
}

func post(fname string) error {
	src, err := os.ReadFile(fname)
	if err != nil {
		panic(err)
	}

	//src := strings.NewReader("Jason Christopher Logelin was programming on the dark side of the moon")

	cp := &commp.Calc{}

	fmt.Println("starting cp.Write")
	start := time.Now()
	cp.Write(src)
	fmt.Println("cp.Write duration:", time.Since(start))

	fmt.Println("\nstarting commP")
	start = time.Now()
	commP, paddedPieceSize, err := cp.Digest()
	if err != nil {
		return err
	}

	p, _ := commcid.PieceCommitmentV1ToCID(commP)

	fmt.Println("generated commP", "duration:", time.Since(start), "commP:", p.String(), "pps:", paddedPieceSize, "mbps:", float64(benchSize)/time.Since(start).Seconds()/1024/1024)

	return nil
}

func writeFiles(ctx context.Context, bs *blockstore.ReadWrite, paths ...string) (cid.Cid, error) {
	//cpBuffer := &bytes.Buffer{}

	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := bs.Get(ctx, cl.Cid)
		if err != nil {
			return nil, err
		}

		//cp.Write(blk.Cid().Prefix().Bytes())
		//cp.Write(blk.RawData())
		//cp.Write(blk.Cid().Hash())

		//cp.Write(blk.Cid().Bytes())
		return bytes.NewBuffer(blk.RawData()), nil
	}
	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			bs.Put(ctx, blk)

			//cp.Write(cl.Bytes())
			//cpBuffer.Write(blk.Cid().Bytes())
			//cpBuffer.Write(blk.RawData())
			//cp.Write([]byte(l.Binary()))

			//cp.Write(blk.Cid().Prefix().Bytes())
			//cp.Write(blk.RawData())
			//cp.Write(blk.Cid().Hash())

			//cp.Write(blk.Cid().Bytes())

			//cp.Write(blk.Cid().Prefix().Bytes())

			return nil
		}, nil
	}

	topLevel := make([]dagpb.PBLink, 0, len(paths))
	for _, p := range paths {
		l, size, err := builder.BuildUnixFSRecursive(p, &ls)
		if err != nil {
			return cid.Undef, err
		}
		name := path.Base(p)
		entry, err := builder.BuildUnixFSDirectoryEntry(name, int64(size), l)
		if err != nil {
			return cid.Undef, err
		}
		topLevel = append(topLevel, entry)
	}

	// make a directory for the file(s).
	root, _, err := builder.BuildUnixFSDirectory(topLevel, &ls)
	if err != nil {
		return cid.Undef, err
	}
	rcl, ok := root.(cidlink.Link)
	if !ok {
		return cid.Undef, fmt.Errorf("could not interpret %s", root)
	}

	return rcl.Cid, nil
}

func allSelector() ipldprime.Node {
	ssb := buildr.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).
		Node()
}

func ReplaceRootsInBuffer(f *bytes.Buffer, roots []cid.Cid, opts ...car.Option) (*bytes.Buffer, error) {
	//var b bb.Buffer
	/*
		f, err := os.OpenFile(path, os.O_RDWR, 0o666)
		if err != nil {
			return err
		}
		defer func() {
			// Close file and override return error type if it is nil.
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}()
	*/

	newHeader := &carv1.CarHeader{
		Roots:   roots,
		Version: 1,
	}
	// Serialize the new header straight up instead of using carv1.HeaderSize.
	// Because, carv1.HeaderSize serialises it to calculate size anyway.
	// By serializing straight up we get the replacement bytes and size.
	// Otherwise, we end up serializing the new header twice:
	// once through carv1.HeaderSize, and
	// once to write it out.
	var buf bytes.Buffer
	if err := WriteHeader(newHeader, &buf); err != nil {
		return nil, err
	}

	nbuf := bytes.NewBuffer(buf.Bytes())
	nbuf.Write(f.Bytes())

	//_, err = f.Write(buf.Bytes())
	return nbuf, nil
}

type readerPlusByte struct {
	io.Reader

	byteBuf [1]byte // escapes via io.Reader.Read; preallocate
}

func ToByteReader(r io.Reader) io.ByteReader {
	if br, ok := r.(io.ByteReader); ok {
		return br
	}
	return &readerPlusByte{Reader: r}
}

func (rb *readerPlusByte) ReadByte() (byte, error) {
	_, err := io.ReadFull(rb, rb.byteBuf[:])
	return rb.byteBuf[0], err
}

func LdReadSize(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) (uint64, error) {
	l, err := varint.ReadUvarint(ToByteReader(r))
	if err != nil {
		// If the length of bytes read is non-zero when the error is EOF then signal an unclean EOF.
		if l > 0 && err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	} else if l == 0 && zeroLenAsEOF {
		return 0, io.EOF
	}

	if l > maxReadBytes { // Don't OOM
		return 0, ErrSectionTooLarge
	}
	return l, nil
}

func LdRead(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) ([]byte, error) {
	l, err := LdReadSize(r, zeroLenAsEOF, maxReadBytes)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

var ErrSectionTooLarge = errors.New("invalid section data, length of read beyond allowable maximum")
var ErrHeaderTooLarge = errors.New("invalid header data, length of read beyond allowable maximum")

func ReadHeader(r io.Reader, maxReadBytes uint64) (*carv1.CarHeader, error) {
	hb, err := LdRead(r, false, maxReadBytes)
	if err != nil {
		if err == ErrSectionTooLarge {
			err = ErrHeaderTooLarge
		}
		return nil, err
	}

	var ch carv1.CarHeader
	if err := cbor.DecodeInto(hb, &ch); err != nil {
		return nil, fmt.Errorf("invalid header: %v", err)
	}

	return &ch, nil
}

func WriteHeader(h *carv1.CarHeader, w io.Writer) error {
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return err
	}

	return util.LdWrite(w, hb)
}
