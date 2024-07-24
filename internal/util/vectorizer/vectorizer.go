/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package vectorizer

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
)

type Vectorizer interface {
	Vectorize(meta storage.ChannelStats, data ...string) (int64, [][]byte, error)
	GetField() *schemapb.FieldSchema
}

type HashVectorizer struct {
	field     *schemapb.FieldSchema
	tokenizer tokenizerapi.Tokenizer
}

func NewHashVectorizer(field *schemapb.FieldSchema, tokenizer tokenizerapi.Tokenizer) *HashVectorizer {
	return &HashVectorizer{
		field:     field,
		tokenizer: tokenizer,
	}
}

func (v *HashVectorizer) Vectorize(meta storage.ChannelStats, data ...string) (int64, [][]byte, error) {
	row := len(data)

	dim := int64(0)
	embedData := make([][]byte, row)
	for i := 0; i < row; i++ {
		rowData := data[i]
		embeddingMap := map[uint32]int32{}
		tokenStream := v.tokenizer.NewTokenStream(rowData)
		for tokenStream.Advance() {
			token := tokenStream.Token()
			// TODO More Hash Option
			hash := typeutil.HashString2Uint32(token)
			embeddingMap[hash] += 1
		}
		if meta != nil {
			meta.Append(rowData, embeddingMap)
		}

		if vectorDim := int64(len(embeddingMap)); vectorDim > dim {
			dim = vectorDim
		}

		embedData[i] = typeutil.CreateSparseFloatRow(lo.Keys(embeddingMap), lo.Map(lo.Values(embeddingMap), func(num int32, _ int) float32 { return float32(num) }))
	}
	return dim, embedData, nil
}

func (v *HashVectorizer) GetField() *schemapb.FieldSchema {
	return v.field
}
