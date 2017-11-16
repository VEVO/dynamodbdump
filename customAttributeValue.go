package main

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// CustomAttributeValue is here to get rid of the empty fields during the json
// Marshaling as the dynamodb.AttributeValue don't have any json tag set
type CustomAttributeValue struct {
	B    []byte                           `json:"b,omitempty"`
	BOOL *bool                            `json:"bool,omitempty"`
	BS   [][]byte                         `json:"bs,omitempty"`
	N    *string                          `json:"n,omitempty"`
	NS   []*string                        `json:"ns,omitempty"`
	NULL *bool                            `json:"null,omitempty"`
	S    *string                          `json:"s,omitempty"`
	SS   []*string                        `json:"ss,omitempty"`
	L    []*CustomAttributeValue          `json:"l,omitempty"`
	M    map[string]*CustomAttributeValue `json:"m,omitempty"`
}

// Unmarshal translates the current CustomAttributeValue to a *dynamodb.AttributeValue
func (attr *CustomAttributeValue) Unmarshal(out *dynamodb.AttributeValue) {
	out.B = attr.B
	out.BOOL = attr.BOOL
	out.BS = attr.BS
	out.N = attr.N
	out.NS = attr.NS
	out.NULL = attr.NULL
	out.S = attr.S
	out.SS = attr.SS
	if len(attr.L) > 0 {
		out.L = []*dynamodb.AttributeValue{}
	}
	for _, child := range attr.L {
		translated := dynamodb.AttributeValue{}
		child.Unmarshal(&translated)
		out.L = append(out.L, &translated)
	}

	if len(attr.M) > 0 {
		out.M = make(map[string]*dynamodb.AttributeValue)
	}
	for k, v := range attr.M {
		translated := dynamodb.AttributeValue{}
		v.Unmarshal(&translated)
		out.M[k] = &translated
	}
}

// Marshal translates a *dynamodb.AttributeValue into a
// *CustomAttributeValue field for field
func (attr *CustomAttributeValue) Marshal(in *dynamodb.AttributeValue) {
	attr.B = in.B
	attr.BOOL = in.BOOL
	attr.BS = in.BS
	attr.N = in.N
	attr.NS = in.NS
	attr.NULL = in.NULL
	attr.S = in.S
	attr.SS = in.SS

	if len(in.L) > 0 {
		attr.L = []*CustomAttributeValue{}
	}
	for _, child := range in.L {
		translated := CustomAttributeValue{}
		translated.Marshal(child)
		attr.L = append(attr.L, &translated)
	}

	if len(in.M) > 0 {
		attr.M = make(map[string]*CustomAttributeValue)
	}
	for k, v := range in.M {
		translated := CustomAttributeValue{}
		translated.Marshal(v)
		attr.M[k] = &translated
	}
}

// MarshalDynamoAttributeMap only purpose is to create a json-format
// representation of the AttributeValue map without the empty fields. We
// wouldn't need that if the fields of the struct was set to
// `json:",omitempty"` and we can't use the dynamodbattribute package because
// you can't really do a field for field copy with it.
func MarshalDynamoAttributeMap(attrs map[string]*dynamodb.AttributeValue) ([]byte, error) {
	resultMap := make(map[string]*CustomAttributeValue)

	for k, v := range attrs {
		custAttr := CustomAttributeValue{}
		custAttr.Marshal(v)
		resultMap[k] = &custAttr
	}
	return json.Marshal(resultMap)
}
