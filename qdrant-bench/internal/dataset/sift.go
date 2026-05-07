// Package dataset reads the SIFT-128-Euclidean ANN benchmark dataset from
// HDF5 format. The file contains three datasets:
//
//   - "train"     : float32  shape (1_000_000, 128)  -- corpus vectors
//   - "test"      : float32  shape (   10_000, 128)  -- query vectors
//   - "neighbors" : int32    shape (   10_000,  100) -- ground-truth top-100
//                                                     nearest-neighbor indices
//                                                     into the train split,
//                                                     L2-sorted ascending.
//   - "distances" : float32  shape (   10_000,  100) -- the corresponding L2
//                                                     distances (we don't
//                                                     need this for recall@k)
//
// We expose those as []float32 flat buffers + dimensions, which is the form
// the Qdrant client expects.
package dataset

import (
	"fmt"
	"os"
	"path/filepath"

	"gonum.org/v1/hdf5"
)

// SIFT holds the loaded dataset.  Vectors are stored row-major, packed into
// a single []float32 of length numRows*Dim. Index i runs from 0..numRows-1
// and its vector is buf[i*Dim : (i+1)*Dim].
type SIFT struct {
	Dim          int
	Train        []float32 // 1_000_000 * 128
	NumTrain     int
	Test         []float32 // 10_000 * 128
	NumTest      int
	Neighbors    []int32 // 10_000 * 100  (ground-truth)
	NumNeighbors int     // = 100
}

// TrainVec returns a slice view (no copy) of training vector i.
func (s *SIFT) TrainVec(i int) []float32 {
	return s.Train[i*s.Dim : (i+1)*s.Dim]
}

// TestVec returns a slice view (no copy) of query vector i.
func (s *SIFT) TestVec(i int) []float32 {
	return s.Test[i*s.Dim : (i+1)*s.Dim]
}

// NeighborsOf returns the int32 ground-truth neighbor IDs for query i.
// Length is s.NumNeighbors (100 for SIFT).
func (s *SIFT) NeighborsOf(i int) []int32 {
	return s.Neighbors[i*s.NumNeighbors : (i+1)*s.NumNeighbors]
}

// Load reads sift-128-euclidean.hdf5 from disk. The path argument should point
// at the file (NOT a directory).
func Load(path string) (*SIFT, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("dataset: resolve path: %w", err)
	}
	if _, err := os.Stat(abs); err != nil {
		return nil, fmt.Errorf("dataset: file not found at %s: %w (run `make data` to download)", abs, err)
	}

	f, err := hdf5.OpenFile(abs, hdf5.F_ACC_RDONLY)
	if err != nil {
		return nil, fmt.Errorf("dataset: open hdf5: %w", err)
	}
	defer f.Close()

	train, trainRows, trainCols, err := read2DFloat32(f, "train")
	if err != nil {
		return nil, fmt.Errorf("dataset: read train: %w", err)
	}
	test, testRows, testCols, err := read2DFloat32(f, "test")
	if err != nil {
		return nil, fmt.Errorf("dataset: read test: %w", err)
	}
	if trainCols != testCols {
		return nil, fmt.Errorf("dataset: dimension mismatch: train=%d test=%d", trainCols, testCols)
	}
	neighbors, _, neighborsCols, err := read2DInt32(f, "neighbors")
	if err != nil {
		return nil, fmt.Errorf("dataset: read neighbors: %w", err)
	}

	return &SIFT{
		Dim:          trainCols,
		Train:        train,
		NumTrain:     trainRows,
		Test:         test,
		NumTest:      testRows,
		Neighbors:    neighbors,
		NumNeighbors: neighborsCols,
	}, nil
}

// read2DFloat32 reads a 2-D float32 dataset and flattens it into a row-major
// []float32. Returns (buf, numRows, numCols, err).
func read2DFloat32(f *hdf5.File, name string) ([]float32, int, int, error) {
	ds, err := f.OpenDataset(name)
	if err != nil {
		return nil, 0, 0, err
	}
	defer ds.Close()

	space := ds.Space()
	defer space.Close()
	dims, _, err := space.SimpleExtentDims()
	if err != nil {
		return nil, 0, 0, err
	}
	if len(dims) != 2 {
		return nil, 0, 0, fmt.Errorf("expected 2-D dataset, got %dD", len(dims))
	}
	rows, cols := int(dims[0]), int(dims[1])

	// Allocate one big contiguous buffer and read directly into it. gonum/hdf5
	// happily reads a flat []float32 into a contiguous 2-D dataset.
	buf := make([]float32, rows*cols)
	if err := ds.Read(&buf); err != nil {
		return nil, 0, 0, err
	}
	return buf, rows, cols, nil
}

func read2DInt32(f *hdf5.File, name string) ([]int32, int, int, error) {
	ds, err := f.OpenDataset(name)
	if err != nil {
		return nil, 0, 0, err
	}
	defer ds.Close()

	space := ds.Space()
	defer space.Close()
	dims, _, err := space.SimpleExtentDims()
	if err != nil {
		return nil, 0, 0, err
	}
	if len(dims) != 2 {
		return nil, 0, 0, fmt.Errorf("expected 2-D dataset, got %dD", len(dims))
	}
	rows, cols := int(dims[0]), int(dims[1])

	buf := make([]int32, rows*cols)
	if err := ds.Read(&buf); err != nil {
		return nil, 0, 0, err
	}
	return buf, rows, cols, nil
}
