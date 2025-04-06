package mocks

import (
	pmsp "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/msp"
	"github.com/stretchr/testify/mock"
)

type ACLProvider struct {
	mock.Mock
}

func (m *ACLProvider) CheckACL(resName string, channelID string, idinfo interface{}) error {
	args := m.Called(resName, channelID, idinfo)
	return args.Error(0)
}

func (m *ACLProvider) GenerateSimulationResults(txEnvelope *peer.SignedProposal, simulation []byte, chainID string) error {
	args := m.Called(txEnvelope, simulation, chainID)
	return args.Error(0)
}

func (m *ACLProvider) DeserializeIdentity(serializedIdentity []byte) (msp.Identity, error) {
	args := m.Called(serializedIdentity)
	return args.Get(0).(msp.Identity), args.Error(1)
}

func (m *ACLProvider) IsWellFormed(identity *pmsp.SerializedIdentity) error {
	args := m.Called(identity)
	return args.Error(0)
}

// Ensure ACLProvider implements msp.IdentityDeserializer
var _ msp.IdentityDeserializer = (*ACLProvider)(nil)
