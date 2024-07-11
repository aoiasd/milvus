// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type EmbeddingData struct {
	Data map[FieldID]FieldData // ONLY SUPPORT SPARSE VECTOR

}

func NewEmbeddingData(schema *schemapb.CollectionSchema) (*EmbeddingData, error) {
	return NewEmbeddingDataWithCap(schema, 0)
}

func NewEmbeddingDataWithCap(schema *schemapb.CollectionSchema, cap int) (*EmbeddingData, error) {
	if schema == nil {
		return nil, merr.WrapErrParameterMissing("collection schema")
	}

	edata := &EmbeddingData{
		Data: make(map[FieldID]FieldData),
	}

	for _, field := range schema.GetFields() {
		// TODO
		// if !field.IsMatch() {
		// 	continue
		// }
		fieldData, err := NewFieldData(schemapb.DataType_SparseFloatVector, field, cap)
		if err != nil {
			return nil, err
		}
		edata.Data[field.FieldID] = fieldData
	}

	return edata, nil
}

type EmbeddingWriter struct {
	offset int
	output *bytes.Buffer
	// TODO Embedding Type?
	embeddingType schemapb.DataType

	// size of single pair of indice and values
	pairSize int

	isFinished bool
}

func NewEmbeddingWriter(embeddingType schemapb.DataType) *EmbeddingWriter {
	return &EmbeddingWriter{
		embeddingType: embeddingType,
	}
}

func (writer *EmbeddingWriter) WriteRow(content []byte) {
	writer.output.Write(binary.LittleEndian.AppendUint32(make([]byte, 4), uint32(len(content)/writer.pairSize)))
	writer.output.Write(content)
}

func (writer *EmbeddingWriter) Write(data FieldData) (int, error) {
	for i := 0; i < data.RowNum(); i++ {
		row := data.GetRow(i)
		vectorBytes, ok := row.([]byte)
		if !ok {
			return 0, merr.WrapErrParameterInvalid("sparse vector", row, "Wrong row type")
		}
		writer.WriteRow(vectorBytes)
	}
	return data.RowNum(), nil
}

func (writer *EmbeddingWriter) GetBuffer() ([]byte, error) {
	if !writer.isFinished {
		return nil, errors.New("please close embedding writer before get buffer")
	}

	data := writer.output.Bytes()
	if len(data) == 0 {
		return nil, errors.New("empty buffer")
	}

	return data, nil
}

func (writer *EmbeddingWriter) Finish() {
	writer.isFinished = true
}

type EmebddingReader struct {
}
