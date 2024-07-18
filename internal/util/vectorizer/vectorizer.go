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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type Vectorizer interface {
	Vectorize(data storage.FieldData, meta storage.EmbeddingMeta) (storage.FieldData, error)
}

type HashVectorizer struct {
	field     *schemapb.FieldSchema
	tokenizer tokenizerapi.Tokenizer
}

func (v *HashVectorizer) Vectorize(data storage.FieldData, meta storage.EmbeddingMeta) (storage.FieldData, error) {
	embedData, err := storage.NewFieldData(v.field.GetDataType(), v.field, data.RowNum())
	if err != nil {
		return nil, fmt.Errorf("create field data failed", zap.String("dataType", v.field.GetDataType().String()))
	}

	for i := 0; i < data.RowNum(); i++ {
		rowData, ok := data.GetRow(i).(string)
		if !ok {
			// TODO
			return nil, fmt.Errorf("")
		}

		embeddingMap := map[uint32]int32{}
		tokenStream := v.tokenizer.NewTokenStream(rowData)
		for tokenStream.Advance() {
			token := tokenStream.Token()
			// TODO More Hash Option
			hash := typeutil.HashString2Uint32(token)
			embeddingMap[hash] += 1
		}
		meta.Append(rowData, embeddingMap)
		embedData.AppendRow(typeutil.CreateSparseFloatRow(lo.Keys(embeddingMap), lo.Map(lo.Values(embeddingMap), func(num int32, _ int) float32 { return float32(num) })))
	}
	return embedData, nil
}
