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
	"sync"

	"github.com/milvus-io/milvus/internal/util/ctokenizer"
	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Vectorizer interface {
	Vectorize(data ...string) (int64, []map[uint32]float32, error)
}

type HashVectorizer struct {
	tokenizer   tokenizerapi.Tokenizer
	concurrency int
}

func NewHashVectorizer(tokenizer tokenizerapi.Tokenizer) *HashVectorizer {
	return &HashVectorizer{
		tokenizer:   tokenizer,
		concurrency: 8,
	}
}

func (v *HashVectorizer) vectorize(data []string, dst []map[uint32]float32) (int64, error) {
	// TODO AOIASD: TOKENIZER CONCURRENT SAFE AND REMOVE INIT TOKENIZER
	tokenizer, err := ctokenizer.NewTokenizer(make(map[string]string))
	if err != nil {
		return 0, err
	}

	dim := int64(0)
	for i := 0; i < len(data); i++ {
		rowData := data[i]
		embeddingMap := map[uint32]float32{}
		tokenStream := tokenizer.NewTokenStream(rowData)
		for tokenStream.Advance() {
			token := tokenStream.Token()
			// TODO More Hash Option
			hash := typeutil.HashString2Uint32(token)
			embeddingMap[hash] += 1
		}

		if vectorDim := int64(len(embeddingMap)); vectorDim > dim {
			dim = vectorDim
		}

		dst[i] = embeddingMap
	}
	return dim, nil
}

func (v *HashVectorizer) Vectorize(data ...string) (int64, []map[uint32]float32, error) {
	row := len(data)
	embedData := make([]map[uint32]float32, row)
	wg := sync.WaitGroup{}

	errCh := make(chan error, v.concurrency)
	dimCh := make(chan int64, v.concurrency)
	for i, j := 0, 0; i < v.concurrency && j < row; i++ {
		var start = j
		var end = start + row/v.concurrency
		if i < row%v.concurrency {
			end += 1
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			dim, err := v.vectorize(data[start:end], embedData[start:end])
			if err != nil {
				errCh <- err
				return
			}
			dimCh <- dim
		}()
		j = end
	}

	wg.Wait()
	close(errCh)
	close(dimCh)
	for err := range errCh {
		if err != nil {
			return 0, nil, err
		}
	}

	maxDim := int64(0)
	for dim := range dimCh {
		if dim > maxDim {
			maxDim = dim
		}
	}
	return maxDim, embedData, nil
}
