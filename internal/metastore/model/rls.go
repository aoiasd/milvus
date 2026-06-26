package model

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

type RLSPolicy struct {
	DBID         int64
	CollectionID int64
	PolicyID     int64
	PolicyName   string
	PolicyType   milvuspb.RowPolicyType
	Actions      []milvuspb.RowPolicyAction
	UsingExpr    string
	CheckExpr    string
	Description  string
}

func MarshalRLSPolicyModel(policy *RLSPolicy) *rootcoordpb.RLSPolicyInfo {
	if policy == nil {
		return nil
	}
	return &rootcoordpb.RLSPolicyInfo{
		DbId:         policy.DBID,
		CollectionId: policy.CollectionID,
		PolicyId:     policy.PolicyID,
		PolicyName:   policy.PolicyName,
		PolicyType:   policy.PolicyType,
		Actions:      cloneRowPolicyActions(policy.Actions),
		UsingExpr:    policy.UsingExpr,
		CheckExpr:    policy.CheckExpr,
		Description:  policy.Description,
	}
}

func UnmarshalRLSPolicyModel(policy *rootcoordpb.RLSPolicyInfo) *RLSPolicy {
	if policy == nil {
		return nil
	}
	return &RLSPolicy{
		DBID:         policy.GetDbId(),
		CollectionID: policy.GetCollectionId(),
		PolicyID:     policy.GetPolicyId(),
		PolicyName:   policy.GetPolicyName(),
		PolicyType:   policy.GetPolicyType(),
		Actions:      cloneRowPolicyActions(policy.GetActions()),
		UsingExpr:    policy.GetUsingExpr(),
		CheckExpr:    policy.GetCheckExpr(),
		Description:  policy.GetDescription(),
	}
}

func (policy *RLSPolicy) ToMilvusRowPolicy() *milvuspb.RowPolicy {
	if policy == nil {
		return nil
	}
	return &milvuspb.RowPolicy{
		PolicyName:  policy.PolicyName,
		PolicyType:  policy.PolicyType,
		Actions:     cloneRowPolicyActions(policy.Actions),
		UsingExpr:   policy.UsingExpr,
		CheckExpr:   policy.CheckExpr,
		Description: policy.Description,
	}
}

type RLSPrincipal struct {
	DBID          int64
	CollectionID  int64
	PrincipalName string
	Tags          map[string]string
}

func MarshalRLSPrincipalModel(principal *RLSPrincipal) *rootcoordpb.RLSPrincipalInfo {
	if principal == nil {
		return nil
	}
	return &rootcoordpb.RLSPrincipalInfo{
		DbId:          principal.DBID,
		CollectionId:  principal.CollectionID,
		PrincipalName: principal.PrincipalName,
		Tags:          cloneStringMap(principal.Tags),
	}
}

func UnmarshalRLSPrincipalModel(principal *rootcoordpb.RLSPrincipalInfo) *RLSPrincipal {
	if principal == nil {
		return nil
	}
	return &RLSPrincipal{
		DBID:          principal.GetDbId(),
		CollectionID:  principal.GetCollectionId(),
		PrincipalName: principal.GetPrincipalName(),
		Tags:          cloneStringMap(principal.GetTags()),
	}
}

func cloneRowPolicyActions(actions []milvuspb.RowPolicyAction) []milvuspb.RowPolicyAction {
	if actions == nil {
		return nil
	}
	cloned := make([]milvuspb.RowPolicyAction, len(actions))
	copy(cloned, actions)
	return cloned
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
