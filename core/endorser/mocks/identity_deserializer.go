package mocks

import (
	"time"

	pmsp "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/msp"
	"github.com/stretchr/testify/mock"
)

type IdentityDeserializer struct {
	mock.Mock
}

func (m *IdentityDeserializer) DeserializeIdentity(serializedIdentity []byte) (msp.Identity, error) {
	args := m.Called(serializedIdentity)
	return args.Get(0).(msp.Identity), args.Error(1)
}

func (m *IdentityDeserializer) IsWellFormed(identity *pmsp.SerializedIdentity) error {
	args := m.Called(identity)
	return args.Error(0)
}

func (m *IdentityDeserializer) CheckACL(resName string, channelID string, idinfo interface{}) error {
	args := m.Called(resName, channelID, idinfo)
	return args.Error(0)
}

func (m *IdentityDeserializer) GenerateSimulationResults(txEnvelope *peer.SignedProposal, simulation []byte, chainID string) error {
	args := m.Called(txEnvelope, simulation, chainID)
	return args.Error(0)
}

// MockIdentity implements msp.Identity interface
type MockIdentity struct {
	mock.Mock
}

func (m *MockIdentity) ExpiresAt() time.Time {
	args := m.Called()
	return args.Get(0).(time.Time)
}

func (m *MockIdentity) GetIdentifier() *msp.IdentityIdentifier {
	args := m.Called()
	return args.Get(0).(*msp.IdentityIdentifier)
}

func (m *MockIdentity) GetMSPIdentifier() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockIdentity) Validate() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockIdentity) GetOrganizationalUnits() []*msp.OUIdentifier {
	args := m.Called()
	return args.Get(0).([]*msp.OUIdentifier)
}

func (m *MockIdentity) Anonymous() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockIdentity) Verify(msg []byte, sig []byte) error {
	args := m.Called(msg, sig)
	return args.Error(0)
}

func (m *MockIdentity) Serialize() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockIdentity) SatisfiesPrincipal(principal *pmsp.MSPPrincipal) error {
	args := m.Called(principal)
	return args.Error(0)
}

// Ensure MockIdentity implements msp.Identity
var _ msp.Identity = (*MockIdentity)(nil)
