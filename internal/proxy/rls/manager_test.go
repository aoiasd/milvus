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

package rls

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestComposeUsingExpr(t *testing.T) {
	policies := []*milvuspb.RowPolicy{
		{
			PolicyName: "p1",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
			UsingExpr:  "tenant == 1",
		},
		{
			PolicyName: "p2",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
			UsingExpr:  "tenant == 2",
		},
		{
			PolicyName: "p3",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypeRestrictive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
			UsingExpr:  "status == \"active\"",
		},
		{
			PolicyName: "search_only",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypeRestrictive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionSearch},
			UsingExpr:  "ignored == true",
		},
	}

	expr := ComposeUsingExpr(policies, milvuspb.RowPolicyAction_RowPolicyActionQuery, "", nil)
	assert.Equal(t, "((tenant == 1) or (tenant == 2)) and ((status == \"active\"))", expr)
	assert.Empty(t, ComposeUsingExpr(policies, milvuspb.RowPolicyAction_RowPolicyActionDelete, "", nil))
}

func TestComposeUsingExprWithPrincipalTags(t *testing.T) {
	policies := []*milvuspb.RowPolicy{
		{
			PolicyName: "p1",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
			UsingExpr:  "dept == $current_principal_tags['dept']",
		},
		{
			PolicyName: "p2",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypeRestrictive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
			UsingExpr:  "owner == $current_principal and region == $current_principal_tags['region']",
		},
	}

	expr := ComposeUsingExpr(policies, milvuspb.RowPolicyAction_RowPolicyActionQuery, "alice", map[string]string{
		"dept":   "sales",
		"region": "us",
	})
	assert.Equal(t, `((dept == "sales")) and ((owner == "alice" and region == "us"))`, expr)

	expr = ComposeUsingExpr(policies, milvuspb.RowPolicyAction_RowPolicyActionQuery, "alice", map[string]string{"dept": "sales"})
	assert.Equal(t, `((dept == "sales")) and ((false))`, expr)
}

func TestCurrentPrincipalDoesNotMatchTagPrefix(t *testing.T) {
	policies := []*milvuspb.RowPolicy{
		{
			PolicyName: "p1",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
			UsingExpr:  "dept == $current_principal_tags['dept']",
		},
	}

	expr := ComposeUsingExpr(policies, milvuspb.RowPolicyAction_RowPolicyActionQuery, "", map[string]string{"dept": "sales"})
	assert.Equal(t, `((dept == "sales"))`, expr)
}

func TestComposeCheckExpr(t *testing.T) {
	policies := []*milvuspb.RowPolicy{
		{
			PolicyName: "p1",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionInsert},
			CheckExpr:  "tenant == 1",
		},
		{
			PolicyName: "p2",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionInsert},
			CheckExpr:  "tenant == 2",
		},
		{
			PolicyName: "p3",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypeRestrictive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionInsert},
			CheckExpr:  "status == \"active\"",
		},
		{
			PolicyName: "query_only",
			PolicyType: milvuspb.RowPolicyType_RowPolicyTypeRestrictive,
			Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
			CheckExpr:  "ignored == true",
		},
	}

	expr := ComposeCheckExpr(policies, milvuspb.RowPolicyAction_RowPolicyActionInsert, "", nil)
	assert.Equal(t, "((tenant == 1) or (tenant == 2)) and ((status == \"active\"))", expr)
	assert.Empty(t, ComposeCheckExpr(policies, milvuspb.RowPolicyAction_RowPolicyActionUpsert, "", nil))
}

func TestManagerSetGetRemove(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()
	helper := newManagerTestSchemaHelper(t)
	policy := &milvuspb.RowPolicy{
		PolicyName: "p1",
		PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
		UsingExpr:  "dept == $current_principal_tags['dept']",
	}
	tags := map[string]string{"dept": "sales"}

	manager.SetRLSPolicy(ctx, "db", 100, policy)
	manager.SetRLSPrincipalTags(ctx, "db", 100, "alice", tags)

	expr, hasPolicies, err := manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	manager.RemoveRLSPrincipalTags(ctx, "db", 100, "alice")
	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	manager.RemoveRLSCollection(ctx, "db", 100)
	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.False(t, hasPolicies)
	assert.Nil(t, expr)
}

func TestManagerMissingEntriesFailClosed(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()
	helper := newManagerTestSchemaHelper(t)

	expr, hasPolicies, err := manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.False(t, hasPolicies)
	assert.Nil(t, expr)

	manager.SetRLSPolicy(ctx, "db", 100, &milvuspb.RowPolicy{
		PolicyName: "p1",
		PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
		UsingExpr:  "dept == $current_principal_tags['dept']",
	})

	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	manager.SetRLSPrincipalTags(ctx, "db", 100, "alice", map[string]string{"region": "us"})
	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	manager.SetRLSPrincipalTags(ctx, "db", 100, "alice", map[string]string{"dept": "sales"})
	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerSetPolicyByNameAccumulatesAndReplaces(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()
	helper := newManagerTestSchemaHelper(t)

	manager.SetRLSPolicy(ctx, "db", 100, &milvuspb.RowPolicy{
		PolicyName: "sales",
		PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
		UsingExpr:  `dept == "sales"`,
	})
	manager.SetRLSPolicy(ctx, "db", 100, &milvuspb.RowPolicy{
		PolicyName: "engineering",
		PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
		UsingExpr:  `dept == "engineering"`,
	})

	expr, hasPolicies, err := manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, expr, "query", "using"))

	manager.SetRLSPolicy(ctx, "db", 100, &milvuspb.RowPolicy{
		PolicyName: "engineering",
		PolicyType: milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:    []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_RowPolicyActionQuery},
		UsingExpr:  `dept == "product"`,
	})

	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("product"), 1, expr, "query", "using"))

	manager.RemoveRLSPolicy(ctx, "db", 100, "sales")
	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.True(t, hasPolicies)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("product"), 1, expr, "query", "using"))

	manager.RemoveRLSPolicy(ctx, "db", 100, "engineering")
	expr, hasPolicies, err = manager.GetRLSUsingPredicate(ctx, "db", "coll", 100, "alice", milvuspb.RowPolicyAction_RowPolicyActionQuery, helper, nil)
	require.NoError(t, err)
	assert.False(t, hasPolicies)
	assert.Nil(t, expr)
}

func newManagerTestSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()

	schema := &schemapb.CollectionSchema{
		Name: "rls_manager_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func managerTestFieldsData(dept string) []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
		},
		{
			FieldId:   101,
			FieldName: "dept",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{dept}}}}},
		},
	}
}

func TestMergeExprToPlan(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_plan_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{
				FieldID:  103,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	visitorArgs := &planparserv2.ParserVisitorArgs{}

	retrievePlan, err := planparserv2.CreateRetrievePlanArgs(helper, "age > 18", nil, visitorArgs)
	require.NoError(t, err)
	require.NoError(t, MergeExprToPlan(helper, retrievePlan, `owner == "alice"`, visitorArgs))
	assertPredicateMerged(t, retrievePlan.GetQuery().GetPredicates())

	searchPlan, err := planparserv2.CreateSearchPlanArgs(helper, "age > 18", "vec", &planpb.QueryInfo{
		Topk:           10,
		MetricType:     "L2",
		SearchParams:   "{}",
		GroupByFieldId: -1,
	}, nil, nil, visitorArgs)
	require.NoError(t, err)
	require.NoError(t, MergeExprToPlan(helper, searchPlan, `owner == "alice"`, visitorArgs))
	assertPredicateMerged(t, searchPlan.GetVectorAnns().GetPredicates())
}

func assertPredicateMerged(t *testing.T, expr *planpb.Expr) {
	t.Helper()

	binaryExpr := expr.GetBinaryExpr()
	require.NotNil(t, binaryExpr)
	assert.Equal(t, planpb.BinaryExpr_LogicalAnd, binaryExpr.GetOp())
	assert.NotNil(t, binaryExpr.GetLeft())
	assert.NotNil(t, binaryExpr.GetRight())
}

func TestValidateRows(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
		},
		{
			FieldId:   101,
			FieldName: "owner",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"alice", "alice"}}}}},
		},
		{
			FieldId:   102,
			FieldName: "age",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18, 19}}}}},
		},
		{
			FieldId:   103,
			FieldName: "tags",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red", "blue"}}}},
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}},
				},
			}}}},
		},
	}

	err = ValidateRows(context.Background(), fieldsData, helper, 2, `owner == "alice" and age in [18, 19] and array_contains(tags, "red")`, "insert", "check")
	require.NoError(t, err)

	err = ValidateRows(context.Background(), fieldsData, helper, 2, `age == 18`, "insert", "check")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "row 1")
}
