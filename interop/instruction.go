package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type ExperimentParams struct {
	Script ScriptInstructions `json:"script"`
}

type ScriptInstruction interface {
	isInstruction()
}

type Connect struct {
	Type      string `json:"type"`
	ConnectTo []int  `json:"connectTo"`
}

type IfNodeIDEquals struct {
	Type        string            `json:"type"`
	NodeID      int               `json:"nodeID"`
	Instruction ScriptInstruction `json:"instruction"`
}

type WaitUntil struct {
	Type           string `json:"type"`
	ElapsedSeconds int    `json:"elapsedSeconds"`
}

type SubscribeToTopic struct {
	Type    string `json:"type"`
	TopicID string `json:"topicID"`
}

type InitWaggle struct {
	Type         string       `json:"type"`
	WaggleParams WaggleParams `json:"waggleParams"`
}

// WaggleParams fields are nil when the experiment does not set them.
type WaggleParams struct {
	PublishFanout *int `json:"publishFanout"`
	GossipFanout  *int `json:"gossipFanout"`
}

type AddPiece struct {
	Type     string `json:"type"`
	Parts    uint8  `json:"parts"`
	TopicID  string `json:"topicID"`
	ObjectID uint64 `json:"objectID"`
}

type Publish struct {
	Type     string `json:"type"`
	TopicID  string `json:"topicID"`
	ObjectID uint64 `json:"objectID"`
}

func (Connect) isInstruction()          {}
func (IfNodeIDEquals) isInstruction()   {}
func (WaitUntil) isInstruction()        {}
func (SubscribeToTopic) isInstruction() {}
func (InitWaggle) isInstruction()       {}
func (AddPiece) isInstruction()         {}
func (Publish) isInstruction()          {}

func (i *IfNodeIDEquals) UnmarshalJSON(data []byte) error {
	var v struct {
		Type        string          `json:"type"`
		NodeID      int             `json:"nodeID"`
		Instruction json.RawMessage `json:"instruction"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	in, err := unmarshalInstruction(v.Instruction)
	if err != nil {
		return err
	}
	*i = IfNodeIDEquals{Type: v.Type, NodeID: v.NodeID, Instruction: in}
	return nil
}

type ScriptInstructions []ScriptInstruction

func (si *ScriptInstructions) UnmarshalJSON(data []byte) error {
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		return err
	}
	out := make(ScriptInstructions, 0, len(raws))
	for _, raw := range raws {
		in, err := unmarshalInstruction(raw)
		if err != nil {
			return err
		}
		out = append(out, in)
	}
	*si = out
	return nil
}

// unmarshalInstruction returns a pointer to the instruction struct for the type field.
func unmarshalInstruction(data []byte) (ScriptInstruction, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, err
	}
	var in ScriptInstruction
	switch head.Type {
	case "connect":
		in = &Connect{}
	case "ifNodeIDEquals":
		in = &IfNodeIDEquals{}
	case "waitUntil":
		in = &WaitUntil{}
	case "subscribeToTopic":
		in = &SubscribeToTopic{}
	case "initWaggle":
		in = &InitWaggle{}
	case "addPiece":
		in = &AddPiece{}
	case "publish":
		in = &Publish{}
	default:
		return nil, fmt.Errorf("unknown instruction type: %q", head.Type)
	}
	if err := json.Unmarshal(data, in); err != nil {
		return nil, err
	}
	return in, nil
}

func readParams(path string) (ExperimentParams, error) {
	if path == "" {
		return ExperimentParams{}, fmt.Errorf("params file must be set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ExperimentParams{}, err
	}
	var params ExperimentParams
	if err := json.Unmarshal(data, &params); err != nil {
		return ExperimentParams{}, fmt.Errorf("decode params file: %w", err)
	}
	return params, nil
}
