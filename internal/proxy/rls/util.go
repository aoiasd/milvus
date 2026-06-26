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
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	currentPrincipalPattern    = regexp.MustCompile(`\$current_principal\b`)
	currentPrincipalTagPattern = regexp.MustCompile(`\$current_principal_tags\['([^']+)'\]`)
)

const rlsPrincipalTemplateName = "__rls_principal"

type compiledExpression struct {
	expr           *planpb.Expr
	needsPrincipal bool
	tagVariables   map[string]string
}

func ComposeUsingExpr(policies []*milvuspb.RowPolicy, action milvuspb.RowPolicyAction, principalName string, principalTags map[string]string) string {
	return ComposeExpr(policies, action, principalName, principalTags, func(policy *milvuspb.RowPolicy) string {
		return policy.GetUsingExpr()
	})
}

func ComposeCheckExpr(policies []*milvuspb.RowPolicy, action milvuspb.RowPolicyAction, principalName string, principalTags map[string]string) string {
	return ComposeExpr(policies, action, principalName, principalTags, func(policy *milvuspb.RowPolicy) string {
		return policy.GetCheckExpr()
	})
}

func ComposeExpr(policies []*milvuspb.RowPolicy, action milvuspb.RowPolicyAction, principalName string, principalTags map[string]string, exprSelector func(*milvuspb.RowPolicy) string) string {
	permissiveExprs := make([]string, 0)
	restrictiveExprs := make([]string, 0)
	for _, policy := range policies {
		if !PolicyMatchesAction(policy, action) {
			continue
		}
		policyExpr := strings.TrimSpace(exprSelector(policy))
		if policyExpr == "" {
			continue
		}
		policyExpr = InstantiateExpr(policyExpr, principalName, principalTags)
		switch policy.GetPolicyType() {
		case milvuspb.RowPolicyType_RowPolicyTypePermissive:
			permissiveExprs = append(permissiveExprs, policyExpr)
		case milvuspb.RowPolicyType_RowPolicyTypeRestrictive:
			restrictiveExprs = append(restrictiveExprs, policyExpr)
		}
	}

	groups := make([]string, 0, 2)
	if len(permissiveExprs) > 0 {
		groups = append(groups, joinExprs(permissiveExprs, "or"))
	}
	if len(restrictiveExprs) > 0 {
		groups = append(groups, joinExprs(restrictiveExprs, "and"))
	}
	return joinExprs(groups, "and")
}

func ComposeExprTemplate(policies []*milvuspb.RowPolicy, action milvuspb.RowPolicyAction, exprSelector func(*milvuspb.RowPolicy) string) (string, bool, map[string]string) {
	permissiveExprs := make([]string, 0)
	restrictiveExprs := make([]string, 0)
	needsPrincipal := false
	tagVariables := map[string]string{}

	for _, policy := range policies {
		if !PolicyMatchesAction(policy, action) {
			continue
		}
		policyExpr := strings.TrimSpace(exprSelector(policy))
		if policyExpr == "" {
			continue
		}
		var policyNeedsPrincipal bool
		policyExpr, policyNeedsPrincipal = toTemplateExpr(policyExpr, tagVariables)
		needsPrincipal = needsPrincipal || policyNeedsPrincipal
		switch policy.GetPolicyType() {
		case milvuspb.RowPolicyType_RowPolicyTypePermissive:
			permissiveExprs = append(permissiveExprs, policyExpr)
		case milvuspb.RowPolicyType_RowPolicyTypeRestrictive:
			restrictiveExprs = append(restrictiveExprs, policyExpr)
		}
	}

	groups := make([]string, 0, 2)
	if len(permissiveExprs) > 0 {
		groups = append(groups, joinExprs(permissiveExprs, "or"))
	}
	if len(restrictiveExprs) > 0 {
		groups = append(groups, joinExprs(restrictiveExprs, "and"))
	}
	return joinExprs(groups, "and"), needsPrincipal, tagVariables
}

func toTemplateExpr(expr string, tagVariables map[string]string) (string, bool) {
	expr = strings.TrimSpace(expr)
	needsPrincipal := currentPrincipalPattern.MatchString(expr)
	if needsPrincipal {
		expr = currentPrincipalPattern.ReplaceAllString(expr, "{"+rlsPrincipalTemplateName+"}")
	}

	replaceTag := func(key string) string {
		variable, ok := tagVariables[key]
		if !ok {
			variable = fmt.Sprintf("__rls_tag_%d", len(tagVariables))
			tagVariables[key] = variable
		}
		return "{" + variable + "}"
	}

	expr = currentPrincipalTagPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := currentPrincipalTagPattern.FindStringSubmatch(match)
		key := submatches[1]
		return replaceTag(key)
	})
	return expr, needsPrincipal
}

func CompileExprTemplate(schemaHelper *typeutil.SchemaHelper, expr string, needsPrincipal bool, tagVariables map[string]string, visitorArgs *planparserv2.ParserVisitorArgs) (*compiledExpression, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	if schemaHelper == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to compile RLS expression template with nil schema helper")
	}
	parsedExpr, err := planparserv2.ParseExprTemplate(schemaHelper, expr, visitorArgs)
	if err != nil {
		return nil, merr.Wrapf(err, "failed to parse RLS expression template")
	}
	return &compiledExpression{
		expr:           parsedExpr,
		needsPrincipal: needsPrincipal,
		tagVariables:   tagVariables,
	}, nil
}

func (e *compiledExpression) Instantiate(principalName string, principalTags map[string]string) (*planpb.Expr, error) {
	if e == nil || e.expr == nil {
		return nil, nil
	}
	if e.needsPrincipal && principalName == "" {
		return alwaysFalsePredicate(), nil
	}

	values := make(map[string]*planpb.GenericValue, len(e.tagVariables)+1)
	if e.needsPrincipal {
		values[rlsPrincipalTemplateName] = planparserv2.NewString(principalName)
	}
	for tagKey, variable := range e.tagVariables {
		tagValue, ok := principalTags[tagKey]
		if !ok {
			return alwaysFalsePredicate(), nil
		}
		values[variable] = planparserv2.NewString(tagValue)
	}

	expr := proto.Clone(e.expr).(*planpb.Expr)
	if err := planparserv2.FillExpressionValue(expr, values); err != nil {
		return nil, err
	}
	return rewriter.RewriteExpr(expr), nil
}

func ExprNeedsPrincipalTags(expr string) bool {
	return currentPrincipalTagPattern.MatchString(expr)
}

func InstantiateUsingExpr(expr string, principalName string, principalTags map[string]string) string {
	return InstantiateExpr(expr, principalName, principalTags)
}

func InstantiateCheckExpr(expr string, principalName string, principalTags map[string]string) string {
	return InstantiateExpr(expr, principalName, principalTags)
}

func InstantiateExpr(expr string, principalName string, principalTags map[string]string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}

	if currentPrincipalPattern.MatchString(expr) {
		if principalName == "" {
			return "false"
		}
		expr = currentPrincipalPattern.ReplaceAllString(expr, quoteStringLiteral(principalName))
	}

	missingTag := false
	replaceTag := func(key string) string {
		value, ok := principalTags[key]
		if !ok {
			missingTag = true
			return ""
		}
		return quoteStringLiteral(value)
	}

	expr = currentPrincipalTagPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := currentPrincipalTagPattern.FindStringSubmatch(match)
		key := submatches[1]
		return replaceTag(key)
	})
	if missingTag {
		return "false"
	}
	return expr
}

func PolicyMatchesAction(policy *milvuspb.RowPolicy, action milvuspb.RowPolicyAction) bool {
	if policy == nil {
		return false
	}
	for _, policyAction := range policy.GetActions() {
		if policyAction == action {
			return true
		}
	}
	return false
}

func quoteStringLiteral(value string) string {
	quoted, err := json.Marshal(value)
	if err != nil {
		return `""`
	}
	return string(quoted)
}

func joinExprs(exprs []string, op string) string {
	nonEmpty := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr != "" {
			nonEmpty = append(nonEmpty, parenthesizeExpr(expr))
		}
	}
	if len(nonEmpty) == 0 {
		return ""
	}
	return strings.Join(nonEmpty, " "+op+" ")
}

func parenthesizeExpr(expr string) string {
	return "(" + strings.TrimSpace(expr) + ")"
}

func QueryAction(isIterator bool) milvuspb.RowPolicyAction {
	if isIterator {
		return milvuspb.RowPolicyAction_RowPolicyActionQueryIterator
	}
	return milvuspb.RowPolicyAction_RowPolicyActionQuery
}

func SearchAction(isAdvanced bool, isIterator bool) milvuspb.RowPolicyAction {
	if isAdvanced {
		return milvuspb.RowPolicyAction_RowPolicyActionHybridSearch
	}
	if isIterator {
		return milvuspb.RowPolicyAction_RowPolicyActionSearchIterator
	}
	return milvuspb.RowPolicyAction_RowPolicyActionSearch
}

func MergeExprToPlan(schemaHelper *typeutil.SchemaHelper, plan *planpb.PlanNode, rlsExpr string, visitorArgs *planparserv2.ParserVisitorArgs) error {
	rlsExpr = strings.TrimSpace(rlsExpr)
	if rlsExpr == "" {
		return nil
	}
	rlsPlan, err := planparserv2.CreateRetrievePlanArgs(schemaHelper, rlsExpr, nil, visitorArgs)
	if err != nil {
		return merr.Wrapf(err, "failed to parse RLS using expression")
	}
	return mergePredicateToPlan(plan, rlsPlan.GetQuery().GetPredicates())
}

func MergePredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	return mergePredicateToPlan(plan, rlsPredicate)
}

func mergePredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	if rlsPredicate == nil || isAlwaysTrueExpr(rlsPredicate) {
		return nil
	}
	if plan == nil {
		return merr.WrapErrServiceInternalMsg("failed to merge RLS predicate into nil plan")
	}
	switch node := plan.GetNode().(type) {
	case *planpb.PlanNode_Query:
		node.Query.Predicates = mergePredicate(node.Query.GetPredicates(), rlsPredicate)
	case *planpb.PlanNode_VectorAnns:
		node.VectorAnns.Predicates = mergePredicate(node.VectorAnns.GetPredicates(), rlsPredicate)
	case *planpb.PlanNode_Predicates:
		node.Predicates = mergePredicate(node.Predicates, rlsPredicate)
	default:
		return merr.WrapErrServiceInternalMsg("failed to merge RLS predicate into unsupported plan node %T", node)
	}
	return nil
}

func mergePredicate(userPredicate *planpb.Expr, rlsPredicate *planpb.Expr) *planpb.Expr {
	if userPredicate == nil || isAlwaysTrueExpr(userPredicate) {
		return rlsPredicate
	}
	if rlsPredicate == nil || isAlwaysTrueExpr(rlsPredicate) {
		return userPredicate
	}
	return rewriter.RewriteExpr(&planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:    planpb.BinaryExpr_LogicalAnd,
				Left:  userPredicate,
				Right: rlsPredicate,
			},
		},
	})
}

func isAlwaysTrueExpr(expr *planpb.Expr) bool {
	return expr != nil && expr.GetAlwaysTrueExpr() != nil
}

func alwaysFalsePredicate() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op: planpb.UnaryExpr_Not,
				Child: &planpb.Expr{
					Expr: &planpb.Expr_AlwaysTrueExpr{
						AlwaysTrueExpr: &planpb.AlwaysTrueExpr{},
					},
				},
			},
		},
	}
}

func ValidateCheckForWrite(ctx context.Context, database, collectionName string, collectionID UniqueID, principalName string, action milvuspb.RowPolicyAction, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, operation string) error {
	checkExpr, hasPolicies, err := DefaultManager().GetRLSCheckPredicate(ctx, database, collectionName, collectionID, principalName, action, schemaHelper, nil)
	if err != nil {
		return err
	}
	if !hasPolicies {
		return nil
	}
	if checkExpr == nil {
		return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: no applicable check policies", operation)
	}
	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, checkExpr, operation, "check")
}

func ValidateUsingForExistingRows(ctx context.Context, database, collectionName string, collectionID UniqueID, principalName string, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, operation string) error {
	if rowNum == 0 {
		return nil
	}
	usingExpr, hasPolicies, err := DefaultManager().GetRLSUsingPredicate(ctx, database, collectionName, collectionID, principalName, milvuspb.RowPolicyAction_RowPolicyActionUpsert, schemaHelper, nil)
	if err != nil {
		return err
	}
	return ValidateUsingPredicateForExistingRows(ctx, fieldsData, rowNum, operation, usingExpr, hasPolicies)
}

func ValidateUsingPredicateForExistingRows(ctx context.Context, fieldsData []*schemapb.FieldData, rowNum int, operation string, usingExpr *planpb.Expr, hasPolicies bool) error {
	if rowNum == 0 {
		return nil
	}
	if !hasPolicies {
		return nil
	}
	if usingExpr == nil {
		return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: no applicable using policies", operation)
	}
	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, usingExpr, operation, "using")
}

func ValidateRows(ctx context.Context, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, expr string, operation string, exprKind string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" || rowNum == 0 {
		return nil
	}

	parsedExpr, err := planparserv2.ParseExpr(schemaHelper, expr, nil)
	if err != nil {
		return merr.Wrapf(err, "failed to parse RLS %s expression for %s", exprKind, operation)
	}

	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, parsedExpr, operation, exprKind)
}

func ValidateRowsByPredicate(ctx context.Context, fieldsData []*schemapb.FieldData, rowNum int, parsedExpr *planpb.Expr, operation string, exprKind string) error {
	if parsedExpr == nil || rowNum == 0 {
		return nil
	}

	rowData := newRowData(fieldsData)
	for rowIdx := 0; rowIdx < rowNum; rowIdx++ {
		allowed, err := evalExpr(parsedExpr, rowData, rowIdx)
		if err != nil {
			return merr.Wrapf(err, "failed to evaluate RLS %s expression for %s at row %d", exprKind, operation, rowIdx)
		}
		if !allowed {
			return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS %s expression at row %d", operation, exprKind, rowIdx)
		}
	}
	mlog.Debug(ctx, "RLS row expression validation passed",
		mlog.String("operation", operation), mlog.String("exprKind", exprKind), mlog.Int("rowNum", rowNum))
	return nil
}

type fieldReader struct {
	field *schemapb.FieldData
	iter  func(int) any
}

type rowData struct {
	fields map[int64]*fieldReader
}

func newRowData(fieldsData []*schemapb.FieldData) *rowData {
	data := &rowData{
		fields: make(map[int64]*fieldReader, len(fieldsData)),
	}
	for _, fieldData := range fieldsData {
		if fieldData == nil {
			continue
		}
		data.fields[fieldData.GetFieldId()] = &fieldReader{
			field: fieldData,
			iter:  typeutil.GetDataIterator(fieldData),
		}
	}
	return data
}

func (d *rowData) value(column *planpb.ColumnInfo, rowIdx int) (any, error) {
	if column == nil {
		return nil, merr.WrapErrParameterInvalidMsg("RLS expression has empty column info")
	}
	if len(column.GetNestedPath()) > 0 || column.GetIsElementLevel() {
		return nil, merr.WrapErrParameterInvalidMsg("RLS expression does not support nested or element-level fields")
	}
	reader, ok := d.fields[column.GetFieldId()]
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("RLS expression references field id %d which is not present in row data", column.GetFieldId())
	}
	switch reader.field.GetType() {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_Timestamptz,
		schemapb.DataType_VarChar,
		schemapb.DataType_Text:
		return reader.iter(rowIdx), nil
	case schemapb.DataType_Array:
		return arrayValue(reader.field, rowIdx)
	default:
		return nil, merr.WrapErrParameterInvalidMsg("RLS expression references unsupported field %s with type %s", reader.field.GetFieldName(), reader.field.GetType().String())
	}
}

func evalExpr(expr *planpb.Expr, rowData *rowData, rowIdx int) (bool, error) {
	if expr == nil {
		return false, merr.WrapErrParameterInvalidMsg("RLS expression is empty")
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return true, nil
	case *planpb.Expr_ValueExpr:
		value := node.ValueExpr.GetValue()
		if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok {
			return false, merr.WrapErrParameterInvalidMsg("RLS value expression is not boolean")
		}
		return value.GetBoolVal(), nil
	case *planpb.Expr_UnaryExpr:
		if node.UnaryExpr.GetOp() != planpb.UnaryExpr_Not {
			return false, merr.WrapErrParameterInvalidMsg("unsupported RLS unary operator %s", node.UnaryExpr.GetOp().String())
		}
		allowed, err := evalExpr(node.UnaryExpr.GetChild(), rowData, rowIdx)
		return !allowed, err
	case *planpb.Expr_BinaryExpr:
		return evalBinaryExpr(node.BinaryExpr, rowData, rowIdx)
	case *planpb.Expr_UnaryRangeExpr:
		return evalUnaryRangeExpr(node.UnaryRangeExpr, rowData, rowIdx)
	case *planpb.Expr_TermExpr:
		return evalTermExpr(node.TermExpr, rowData, rowIdx)
	case *planpb.Expr_JsonContainsExpr:
		return evalJSONContainsExpr(node.JsonContainsExpr, rowData, rowIdx)
	default:
		return false, merr.WrapErrParameterInvalidMsg("unsupported RLS expression node %T", node)
	}
}

func evalBinaryExpr(expr *planpb.BinaryExpr, rowData *rowData, rowIdx int) (bool, error) {
	switch expr.GetOp() {
	case planpb.BinaryExpr_LogicalAnd:
		left, err := evalExpr(expr.GetLeft(), rowData, rowIdx)
		if err != nil || !left {
			return left, err
		}
		return evalExpr(expr.GetRight(), rowData, rowIdx)
	case planpb.BinaryExpr_LogicalOr:
		left, err := evalExpr(expr.GetLeft(), rowData, rowIdx)
		if err != nil || left {
			return left, err
		}
		return evalExpr(expr.GetRight(), rowData, rowIdx)
	default:
		return false, merr.WrapErrParameterInvalidMsg("unsupported RLS binary operator %s", expr.GetOp().String())
	}
}

func evalUnaryRangeExpr(expr *planpb.UnaryRangeExpr, rowData *rowData, rowIdx int) (bool, error) {
	rowValue, err := rowData.value(expr.GetColumnInfo(), rowIdx)
	if err != nil {
		return false, err
	}
	if rowValue == nil {
		return false, nil
	}
	return compareValue(rowValue, expr.GetValue(), expr.GetOp())
}

func evalTermExpr(expr *planpb.TermExpr, rowData *rowData, rowIdx int) (bool, error) {
	if expr.GetIsInField() {
		return false, merr.WrapErrParameterInvalidMsg("RLS term expression does not support field-to-field IN")
	}
	rowValue, err := rowData.value(expr.GetColumnInfo(), rowIdx)
	if err != nil {
		return false, err
	}
	if rowValue == nil {
		return false, nil
	}
	for _, value := range expr.GetValues() {
		match, err := valueEqual(rowValue, value)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func evalJSONContainsExpr(expr *planpb.JSONContainsExpr, rowData *rowData, rowIdx int) (bool, error) {
	rowValue, err := rowData.value(expr.GetColumnInfo(), rowIdx)
	if err != nil {
		return false, err
	}
	if rowValue == nil {
		return false, nil
	}
	arrayValue, ok := rowValue.(*schemapb.ScalarField)
	if !ok {
		return false, merr.WrapErrParameterInvalidMsg("RLS contains expression only supports array fields")
	}

	switch expr.GetOp() {
	case planpb.JSONContainsExpr_Contains:
		if len(expr.GetElements()) != 1 {
			return false, merr.WrapErrParameterInvalidMsg("RLS array_contains expression requires exactly one element")
		}
		return scalarArrayContains(arrayValue, expr.GetElements()[0])
	case planpb.JSONContainsExpr_ContainsAll:
		for _, element := range expr.GetElements() {
			contains, err := scalarArrayContains(arrayValue, element)
			if err != nil || !contains {
				return contains, err
			}
		}
		return true, nil
	case planpb.JSONContainsExpr_ContainsAny:
		for _, element := range expr.GetElements() {
			contains, err := scalarArrayContains(arrayValue, element)
			if err != nil {
				return false, err
			}
			if contains {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, merr.WrapErrParameterInvalidMsg("unsupported RLS contains operator %s", expr.GetOp().String())
	}
}

func compareValue(rowValue any, target *planpb.GenericValue, op planpb.OpType) (bool, error) {
	switch op {
	case planpb.OpType_Equal:
		return valueEqual(rowValue, target)
	case planpb.OpType_NotEqual:
		equal, err := valueEqual(rowValue, target)
		return !equal, err
	case planpb.OpType_GreaterThan, planpb.OpType_GreaterEqual, planpb.OpType_LessThan, planpb.OpType_LessEqual:
		order, err := valueCompare(rowValue, target)
		if err != nil {
			return false, err
		}
		switch op {
		case planpb.OpType_GreaterThan:
			return order > 0, nil
		case planpb.OpType_GreaterEqual:
			return order >= 0, nil
		case planpb.OpType_LessThan:
			return order < 0, nil
		case planpb.OpType_LessEqual:
			return order <= 0, nil
		}
	default:
		return false, merr.WrapErrParameterInvalidMsg("unsupported RLS comparison operator %s", op.String())
	}
	return false, merr.WrapErrParameterInvalidMsg("unsupported RLS comparison operator %s", op.String())
}

func valueEqual(rowValue any, target *planpb.GenericValue) (bool, error) {
	if boolVal, ok := rowValue.(bool); ok {
		targetBool, ok := target.GetVal().(*planpb.GenericValue_BoolVal)
		if !ok {
			return false, nil
		}
		return boolVal == targetBool.BoolVal, nil
	}
	if stringVal, ok := rowValue.(string); ok {
		targetString, ok := target.GetVal().(*planpb.GenericValue_StringVal)
		if !ok {
			return false, nil
		}
		return stringVal == targetString.StringVal, nil
	}
	if targetInt, ok := target.GetVal().(*planpb.GenericValue_Int64Val); ok {
		if rowInt, ok := integerValue(rowValue); ok {
			return rowInt == targetInt.Int64Val, nil
		}
		if rowFloat, ok := floatValue(rowValue); ok {
			return rowFloat == float64(targetInt.Int64Val), nil
		}
		return false, nil
	}
	if targetFloat, ok := target.GetVal().(*planpb.GenericValue_FloatVal); ok {
		rowNumber, ok := numericValue(rowValue)
		if !ok {
			return false, nil
		}
		return rowNumber == targetFloat.FloatVal, nil
	}
	return false, merr.WrapErrParameterInvalidMsg("unsupported RLS value type %T", rowValue)
}

func valueCompare(rowValue any, target *planpb.GenericValue) (int, error) {
	if rowString, ok := rowValue.(string); ok {
		targetString, ok := target.GetVal().(*planpb.GenericValue_StringVal)
		if !ok {
			return 0, merr.WrapErrParameterInvalidMsg("RLS comparison type mismatch")
		}
		switch {
		case rowString < targetString.StringVal:
			return -1, nil
		case rowString > targetString.StringVal:
			return 1, nil
		default:
			return 0, nil
		}
	}
	if targetInt, ok := target.GetVal().(*planpb.GenericValue_Int64Val); ok {
		if rowInt, ok := integerValue(rowValue); ok {
			return compareInt(rowInt, targetInt.Int64Val), nil
		}
		if rowFloat, ok := floatValue(rowValue); ok {
			return compareFloat(rowFloat, float64(targetInt.Int64Val)), nil
		}
		return 0, merr.WrapErrParameterInvalidMsg("RLS comparison type mismatch")
	}
	if targetFloat, ok := target.GetVal().(*planpb.GenericValue_FloatVal); ok {
		rowNumber, ok := numericValue(rowValue)
		if !ok {
			return 0, merr.WrapErrParameterInvalidMsg("unsupported RLS ordered comparison value type %T", rowValue)
		}
		return compareFloat(rowNumber, targetFloat.FloatVal), nil
	}
	return 0, merr.WrapErrParameterInvalidMsg("RLS comparison type mismatch")
}

func numericValue(value any) (float64, bool) {
	if value, ok := integerValue(value); ok {
		return float64(value), true
	}
	return floatValue(value)
}

func integerValue(value any) (int64, bool) {
	switch v := value.(type) {
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

func floatValue(value any) (float64, bool) {
	switch v := value.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func compareInt(left int64, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareFloat(left float64, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func genericNumericValue(value *planpb.GenericValue) (float64, bool) {
	switch v := value.GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		return float64(v.Int64Val), true
	case *planpb.GenericValue_FloatVal:
		return v.FloatVal, true
	default:
		return 0, false
	}
}

func arrayValue(field *schemapb.FieldData, rowIdx int) (*schemapb.ScalarField, error) {
	data := field.GetScalars().GetArrayData().GetData()
	dataIdx, valid, err := logicalRowToDataIndex(field.GetValidData(), len(data), rowIdx)
	if err != nil || !valid {
		return nil, err
	}
	return data[dataIdx], nil
}

func logicalRowToDataIndex(validData []bool, dataLen int, rowIdx int) (int, bool, error) {
	if len(validData) == 0 {
		if rowIdx >= dataLen {
			return 0, false, merr.WrapErrParameterInvalidMsg("RLS row index %d exceeds data length %d", rowIdx, dataLen)
		}
		return rowIdx, true, nil
	}
	if rowIdx >= len(validData) {
		return 0, false, merr.WrapErrParameterInvalidMsg("RLS row index %d exceeds valid data length %d", rowIdx, len(validData))
	}
	if !validData[rowIdx] {
		return 0, false, nil
	}
	if dataLen == len(validData) {
		return rowIdx, true, nil
	}
	dataIdx := 0
	for i := 0; i < rowIdx; i++ {
		if validData[i] {
			dataIdx++
		}
	}
	if dataIdx >= dataLen {
		return 0, false, merr.WrapErrParameterInvalidMsg("RLS row index %d maps outside data length %d", rowIdx, dataLen)
	}
	return dataIdx, true, nil
}

func scalarArrayContains(arrayValue *schemapb.ScalarField, target *planpb.GenericValue) (bool, error) {
	switch data := arrayValue.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_BoolVal)
		if !ok {
			return false, nil
		}
		for _, value := range data.BoolData.GetData() {
			if value == targetValue.BoolVal {
				return true, nil
			}
		}
	case *schemapb.ScalarField_IntData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_Int64Val)
		if !ok {
			return false, nil
		}
		for _, value := range data.IntData.GetData() {
			if int64(value) == targetValue.Int64Val {
				return true, nil
			}
		}
	case *schemapb.ScalarField_LongData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_Int64Val)
		if !ok {
			return false, nil
		}
		for _, value := range data.LongData.GetData() {
			if value == targetValue.Int64Val {
				return true, nil
			}
		}
	case *schemapb.ScalarField_FloatData:
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return false, nil
		}
		for _, value := range data.FloatData.GetData() {
			if float64(value) == targetNumber {
				return true, nil
			}
		}
	case *schemapb.ScalarField_DoubleData:
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return false, nil
		}
		for _, value := range data.DoubleData.GetData() {
			if value == targetNumber {
				return true, nil
			}
		}
	case *schemapb.ScalarField_StringData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_StringVal)
		if !ok {
			return false, nil
		}
		for _, value := range data.StringData.GetData() {
			if value == targetValue.StringVal {
				return true, nil
			}
		}
	default:
		return false, merr.WrapErrParameterInvalidMsg("unsupported RLS array element type %T", data)
	}
	return false, nil
}
