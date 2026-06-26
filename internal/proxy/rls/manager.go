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
	"sort"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Manager interface {
	SetRLSPolicy(ctx context.Context, database string, collectionID UniqueID, policy *milvuspb.RowPolicy)
	RemoveRLSPolicy(ctx context.Context, database string, collectionID UniqueID, policyName string)
	RemoveRLSPolicies(ctx context.Context, database string, collectionID UniqueID)

	SetRLSPrincipalTags(ctx context.Context, database string, collectionID UniqueID, principalName string, tags map[string]string)
	RemoveRLSPrincipalTags(ctx context.Context, database string, collectionID UniqueID, principalName string)
	RemoveRLSCollection(ctx context.Context, database string, collectionID UniqueID)

	GetRLSUsingPredicate(ctx context.Context, database, collectionName string, collectionID UniqueID, principalName string, action milvuspb.RowPolicyAction, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, bool, error)
	GetRLSCheckPredicate(ctx context.Context, database, collectionName string, collectionID UniqueID, principalName string, action milvuspb.RowPolicyAction, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, bool, error)
}

type exprKind int

const (
	usingExprKind exprKind = iota
	checkExprKind
)

type compiledKey struct {
	action   milvuspb.RowPolicyAction
	kind     exprKind
	timezone string
}

type manager struct {
	mu            sync.RWMutex
	policies      map[UniqueID]map[string]*milvuspb.RowPolicy
	principalTags map[UniqueID]map[string]map[string]string
	compiled      map[UniqueID]map[compiledKey]*compiledExpression
}

var defaultManager Manager = NewManager()

func NewManager() Manager {
	return &manager{
		policies:      map[UniqueID]map[string]*milvuspb.RowPolicy{},
		principalTags: map[UniqueID]map[string]map[string]string{},
		compiled:      map[UniqueID]map[compiledKey]*compiledExpression{},
	}
}

func DefaultManager() Manager {
	return defaultManager
}

func (m *manager) SetRLSPolicy(ctx context.Context, database string, collectionID UniqueID, policy *milvuspb.RowPolicy) {
	if m == nil || collectionID == 0 || policy == nil || policy.GetPolicyName() == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.policies[collectionID] == nil {
		m.policies[collectionID] = map[string]*milvuspb.RowPolicy{}
	}
	m.policies[collectionID][policy.GetPolicyName()] = policy
	delete(m.compiled, collectionID)
}

func (m *manager) RemoveRLSPolicy(ctx context.Context, database string, collectionID UniqueID, policyName string) {
	if m == nil || collectionID == 0 || policyName == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if policies, ok := m.policies[collectionID]; ok {
		delete(policies, policyName)
		if len(policies) == 0 {
			delete(m.policies, collectionID)
		}
	}
	delete(m.compiled, collectionID)
}

func (m *manager) RemoveRLSPolicies(ctx context.Context, database string, collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.policies, collectionID)
	delete(m.compiled, collectionID)
}

func (m *manager) SetRLSPrincipalTags(ctx context.Context, database string, collectionID UniqueID, principalName string, tags map[string]string) {
	if m == nil || collectionID == 0 || principalName == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.principalTags[collectionID] == nil {
		m.principalTags[collectionID] = map[string]map[string]string{}
	}
	m.principalTags[collectionID][principalName] = tags
}

func (m *manager) RemoveRLSPrincipalTags(ctx context.Context, database string, collectionID UniqueID, principalName string) {
	if m == nil || collectionID == 0 || principalName == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if principals, ok := m.principalTags[collectionID]; ok {
		delete(principals, principalName)
		if len(principals) == 0 {
			delete(m.principalTags, collectionID)
		}
	}
}

func (m *manager) RemoveRLSCollection(ctx context.Context, database string, collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.policies, collectionID)
	delete(m.principalTags, collectionID)
	delete(m.compiled, collectionID)
}

func (m *manager) GetRLSUsingPredicate(ctx context.Context, database, collectionName string, collectionID UniqueID, principalName string, action milvuspb.RowPolicyAction, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, bool, error) {
	return m.getRLSPredicate(ctx, database, collectionName, collectionID, principalName, action, usingExprKind, schemaHelper, visitorArgs, func(policy *milvuspb.RowPolicy) string {
		return policy.GetUsingExpr()
	})
}

func (m *manager) GetRLSCheckPredicate(ctx context.Context, database, collectionName string, collectionID UniqueID, principalName string, action milvuspb.RowPolicyAction, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, bool, error) {
	return m.getRLSPredicate(ctx, database, collectionName, collectionID, principalName, action, checkExprKind, schemaHelper, visitorArgs, func(policy *milvuspb.RowPolicy) string {
		return policy.GetCheckExpr()
	})
}

func (m *manager) getRLSPredicate(ctx context.Context, database, collectionName string, collectionID UniqueID, principalName string, action milvuspb.RowPolicyAction, kind exprKind, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs, exprSelector func(*milvuspb.RowPolicy) string) (*planpb.Expr, bool, error) {
	if m == nil || collectionID == 0 {
		return nil, false, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	policiesByName, ok := m.policies[collectionID]
	if !ok {
		return nil, false, nil
	}
	hasPolicies := len(policiesByName) > 0
	policies := orderedPolicies(policiesByName)
	compiledExpr, err := m.getCompiledExprLocked(collectionID, policies, action, kind, schemaHelper, visitorArgs, exprSelector)
	if err != nil || compiledExpr == nil {
		return nil, hasPolicies, err
	}

	var tags map[string]string
	if principalName != "" {
		if principals, ok := m.principalTags[collectionID]; ok {
			tags = principals[principalName]
		}
	}
	expr, err := compiledExpr.Instantiate(principalName, tags)
	return expr, hasPolicies, err
}

func orderedPolicies(policiesByName map[string]*milvuspb.RowPolicy) []*milvuspb.RowPolicy {
	names := make([]string, 0, len(policiesByName))
	for name := range policiesByName {
		names = append(names, name)
	}
	sort.Strings(names)

	policies := make([]*milvuspb.RowPolicy, 0, len(names))
	for _, name := range names {
		policies = append(policies, policiesByName[name])
	}
	return policies
}

func (m *manager) getCompiledExprLocked(collectionID UniqueID, policies []*milvuspb.RowPolicy, action milvuspb.RowPolicyAction, kind exprKind, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs, exprSelector func(*milvuspb.RowPolicy) string) (*compiledExpression, error) {
	key := compiledKey{
		action: action,
		kind:   kind,
	}
	if visitorArgs != nil {
		key.timezone = visitorArgs.Timezone
	}

	if m.compiled[collectionID] == nil {
		m.compiled[collectionID] = map[compiledKey]*compiledExpression{}
	}
	if compiledExpr, ok := m.compiled[collectionID][key]; ok {
		return compiledExpr, nil
	}

	expr, needsPrincipal, tagVariables := ComposeExprTemplate(policies, action, exprSelector)
	compiledExpr, err := CompileExprTemplate(schemaHelper, expr, needsPrincipal, tagVariables, visitorArgs)
	if err != nil {
		return nil, err
	}
	m.compiled[collectionID][key] = compiledExpr
	return compiledExpr, nil
}
