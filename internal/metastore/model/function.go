package model

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type Function struct {
	Name string
	ID   int64
	Type schemapb.FunctionType

	InputFieldID   []int64
	InputFieldName []string

	OutputFieldID   []int64
	OutputFieldName []string

	Params []*commonpb.KeyValuePair
}

func (f *Function) Clone() *Function {
	return &Function{
		Name: f.Name,
		Type: f.Type,
		ID:   f.ID,

		InputFieldID:   f.InputFieldID,
		InputFieldName: f.InputFieldName,

		OutputFieldID:   f.OutputFieldID,
		OutputFieldName: f.OutputFieldName,
		Params:          f.Params,
	}
}

func (f *Function) Equal(other Function) bool {
	return f.Name == other.Name &&
		f.Type == other.Type &&
		checkNamesEqual(f.InputFieldName, other.InputFieldName) &&
		checkIdsEqual(f.InputFieldID, other.InputFieldID) &&
		checkNamesEqual(f.OutputFieldName, other.OutputFieldName) &&
		checkIdsEqual(f.OutputFieldID, other.OutputFieldID) &&
		checkParamsEqual(f.Params, other.Params)
}

func CloneFunctions(functions []*Function) []*Function {
	clone := make([]*Function, 0, len(functions))
	for _, function := range functions {
		clone = append(clone, function.Clone())
	}
	return clone
}

func MarshalFunctionModel(function *Function) *schemapb.FunctionSchema {
	if function == nil {
		return nil
	}

	return &schemapb.FunctionSchema{
		Name:             function.Name,
		Type:             function.Type,
		Id:               function.ID,
		InputFieldIds:    function.InputFieldID,
		InputFieldNames:  function.InputFieldName,
		OutputFieldIds:   function.OutputFieldID,
		OutputFieldNames: function.OutputFieldName,
		Params:           function.Params,
	}
}

func UnmarshalFunctionModel(schema *schemapb.FunctionSchema) *Function {
	return &Function{
		Name: schema.GetName(),
		ID:   schema.GetId(),
		Type: schema.GetType(),

		InputFieldID:   schema.GetInputFieldIds(),
		InputFieldName: schema.GetInputFieldNames(),

		OutputFieldID:   schema.GetOutputFieldIds(),
		OutputFieldName: schema.GetOutputFieldNames(),
		Params:          schema.GetParams(),
	}
}

func MarshalFunctionModels(functions []*Function) []*schemapb.FunctionSchema {
	if functions == nil {
		return nil
	}

	functionSchemas := make([]*schemapb.FunctionSchema, len(functions))
	for idx, function := range functions {
		functionSchemas[idx] = MarshalFunctionModel(function)
	}
	return functionSchemas
}

func UnmarshalFunctionModels(functions []*schemapb.FunctionSchema) []*Function {
	if functions == nil {
		return nil
	}

	functionSchemas := make([]*Function, len(functions))
	for idx, function := range functions {
		functionSchemas[idx] = UnmarshalFunctionModel(function)
	}
	return functionSchemas
}

func checkNamesEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for id, name := range a {
		if name != b[id] {
			return false
		}
	}
	return true
}

func checkIdsEqual(a []int64, b []int64) bool {
	if len(a) != len(b) {
		return false
	}

	for id, name := range a {
		if name != b[id] {
			return false
		}
	}
	return true
}
