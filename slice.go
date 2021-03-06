/*
Copyright 2019 yametech.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package canal

import (
	"errors"
	"io"
)

type sliceBuffer struct {
	s []byte
	i int
}

func newSliceBuffer(s []byte) *sliceBuffer {
	return &sliceBuffer{s, 0}
}

func (s *sliceBuffer) Slice(n int) ([]byte, error) {
	if s.i+n > len(s.s) {
		return nil, io.EOF
	}
	b := s.s[s.i : s.i+n]
	s.i += n
	return b, nil
}

func (s *sliceBuffer) Skip(n int) {
	if _, err := s.Slice(n); err != nil {
		panic(err)
	}
}

func (s *sliceBuffer) ReadByte() (byte, error) {
	if s.i >= len(s.s) {
		return 0, io.EOF
	}
	b := s.s[s.i]
	s.i++
	return b, nil
}

func (s *sliceBuffer) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if s.i >= len(s.s) {
		return 0, io.EOF
	}
	n := copy(b, s.s[s.i:])
	s.i += n
	return n, nil
}

func (s *sliceBuffer) First(length int) ([]byte, error) {
	buf := make([]byte, length)
	if s.i >= len(s.s) {
		return buf, io.EOF
	}
	copy(buf, s.s[s.i:])
	return buf, nil
}

func (s *sliceBuffer) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(s.i) + offset
	case 2:
		abs = int64(len(s.s)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	if abs >= 1<<31 {
		return 0, errors.New("position out of range")
	}
	s.i = int(abs)
	return abs, nil
}
