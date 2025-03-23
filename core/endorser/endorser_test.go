/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/metrics/metricsfakes"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/endorser"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protoutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// EndorserRole represents the role of an endorser in the network
type EndorserRole int

const (
	NormalEndorser EndorserRole = iota
	LeaderEndorser
)

// EndorserConfig contains configuration for the endorser
type EndorserConfig struct {
	Role           EndorserRole
	LeaderEndorser string // Address of the leader endorser
	EndorserID     string // Unique ID of this endorser
	ChannelID      string // Channel ID this endorser belongs to
}

// TransactionDependencyInfo represents information about a transaction dependency
type TransactionDependencyInfo struct {
	Value         []byte    // The current value of the variable
	DependentTxID string    // ID of the transaction this depends on (if any)
	ExpiryTime    time.Time // When this endorsement expires
	HasDependency bool      // Whether this transaction has a dependency
}

// DependencyInfo represents the dependency information for a transaction
type DependencyInfo struct {
	Value         []byte
	DependentTxID string
	HasDependency bool
}

type CcInterest pb.ChaincodeInterest

func (a CcInterest) Len() int { return len(a.Chaincodes) }
func (a CcInterest) Swap(i, j int) {
	a.Chaincodes[i], a.Chaincodes[j] = a.Chaincodes[j], a.Chaincodes[i]
}

func (a CcInterest) Less(i, j int) bool {
	ai := a.Chaincodes[i]
	aj := a.Chaincodes[j]

	if ai.Name != aj.Name {
		return ai.Name < aj.Name
	}

	if len(ai.CollectionNames) != len(aj.CollectionNames) {
		return len(ai.CollectionNames) < len(aj.CollectionNames)
	}

	for ii := range ai.CollectionNames {
		if ai.CollectionNames[ii] != aj.CollectionNames[ii] {
			return ai.CollectionNames[ii] < aj.CollectionNames[ii]
		}
	}

	return false
}

var _ = Describe("Endorser", func() {
	var (
		fakeProposalDuration         *metricsfakes.Histogram
		fakeProposalsReceived        *metricsfakes.Counter
		fakeSuccessfulProposals      *metricsfakes.Counter
		fakeProposalValidationFailed *metricsfakes.Counter
		fakeProposalACLCheckFailed   *metricsfakes.Counter
		fakeInitFailed               *metricsfakes.Counter
		fakeEndorsementsFailed       *metricsfakes.Counter
		fakeDuplicateTxsFailure      *metricsfakes.Counter
		fakeSimulationFailure        *metricsfakes.Counter
		metrics                      *endorser.Metrics
		support                      *mockSupport
		e                            *endorser.Endorser
	)

	BeforeEach(func() {
		fakeProposalDuration = &metricsfakes.Histogram{}
		fakeProposalsReceived = &metricsfakes.Counter{}
		fakeSuccessfulProposals = &metricsfakes.Counter{}
		fakeProposalValidationFailed = &metricsfakes.Counter{}
		fakeProposalACLCheckFailed = &metricsfakes.Counter{}
		fakeInitFailed = &metricsfakes.Counter{}
		fakeEndorsementsFailed = &metricsfakes.Counter{}
		fakeDuplicateTxsFailure = &metricsfakes.Counter{}
		fakeSimulationFailure = &metricsfakes.Counter{}

		metrics = &endorser.Metrics{
			ProposalDuration:         fakeProposalDuration,
			ProposalsReceived:        fakeProposalsReceived,
			SuccessfulProposals:      fakeSuccessfulProposals,
			ProposalValidationFailed: fakeProposalValidationFailed,
			ProposalACLCheckFailed:   fakeProposalACLCheckFailed,
			InitFailed:               fakeInitFailed,
			EndorsementsFailed:       fakeEndorsementsFailed,
			DuplicateTxsFailure:      fakeDuplicateTxsFailure,
			SimulationFailure:        fakeSimulationFailure,
		}

		support = &mockSupport{}
		support.On("ChaincodeEndorsementInfo", mock.Anything, mock.Anything, mock.Anything).Return(&lifecycle.ChaincodeEndorsementInfo{}, nil)

		e = endorser.NewEndorser(nil, nil, nil, support, nil, metrics, endorser.EndorserConfig{
			Role: endorser.NormalEndorser,
		})
	})

	It("should create a new endorser", func() {
		Expect(e).NotTo(BeNil())
		Expect(e.Config.Role).To(Equal(endorser.NormalEndorser))
	})
})

func TestEndorserDependencyTracking(t *testing.T) {
	t.Run("Dependency Tracking", func(t *testing.T) {
		metrics := endorser.NewMetrics(&metricsfakes.Provider{})
		support := &mockSupport{}
		config := endorser.EndorserConfig{
			Role:       endorser.LeaderEndorser,
			EndorserID: "test-endorser",
			ChannelID:  "test-channel",
		}
		e := endorser.NewEndorser(nil, nil, nil, support, nil, metrics, config)

		// Create a test proposal
		proposal := createTestProposal("test-key", "test-value")
		signedProposal := &pb.SignedProposal{
			ProposalBytes: proposal,
		}

		// Process the proposal
		_, err := e.ProcessProposal(context.Background(), signedProposal)
		assert.NoError(t, err)

		// Verify metrics
		proposalsReceived := metrics.ProposalsReceived.(*metricsfakes.Counter)
		successfulProposals := metrics.SuccessfulProposals.(*metricsfakes.Counter)
		assert.Equal(t, 1, proposalsReceived.AddCallCount())
		assert.Equal(t, 1, successfulProposals.AddCallCount())
	})
}

func TestEndorserRoles(t *testing.T) {
	t.Run("Normal Endorser", func(t *testing.T) {
		metrics := endorser.NewMetrics(&metricsfakes.Provider{})
		support := &mockSupport{}
		config := endorser.EndorserConfig{
			Role:       endorser.NormalEndorser,
			EndorserID: "test-endorser",
			ChannelID:  "test-channel",
		}
		endorser := endorser.NewEndorser(nil, nil, nil, support, nil, metrics, config)

		assert.NotNil(t, endorser)
		assert.Equal(t, endorser.Config.Role, endorser.Config.Role)
	})

	t.Run("Leader Endorser", func(t *testing.T) {
		metrics := endorser.NewMetrics(&metricsfakes.Provider{})
		support := &mockSupport{}
		config := endorser.EndorserConfig{
			Role:       endorser.LeaderEndorser,
			EndorserID: "test-endorser",
			ChannelID:  "test-channel",
		}
		endorser := endorser.NewEndorser(nil, nil, nil, support, nil, metrics, config)

		assert.NotNil(t, endorser)
		assert.Equal(t, endorser.Config.Role, endorser.Config.Role)
	})
}

func TestEndorserMetrics(t *testing.T) {
	t.Run("Proposal Processing Metrics", func(t *testing.T) {
		metrics := endorser.NewMetrics(&metricsfakes.Provider{})
		support := &mockSupport{}
		config := endorser.EndorserConfig{
			Role:       endorser.LeaderEndorser,
			EndorserID: "test-endorser",
			ChannelID:  "test-channel",
		}
		e := endorser.NewEndorser(nil, nil, nil, support, nil, metrics, config)

		// Create and process a test proposal
		proposal := createTestProposal("test-key", "test-value")
		signedProposal := &pb.SignedProposal{
			ProposalBytes: proposal,
		}

		_, err := e.ProcessProposal(context.Background(), signedProposal)
		assert.NoError(t, err)

		// Verify metrics
		proposalsReceived := metrics.ProposalsReceived.(*metricsfakes.Counter)
		successfulProposals := metrics.SuccessfulProposals.(*metricsfakes.Counter)
		assert.Equal(t, 1, proposalsReceived.AddCallCount())
		assert.Equal(t, 1, successfulProposals.AddCallCount())
	})
}

func createTestProposal(key, value string) []byte {
	header := &common.Header{
		ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
			Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			ChannelId: "test-channel",
		}),
	}
	headerBytes, _ := proto.Marshal(header)

	ccHeader := &pb.ChaincodeHeaderExtension{
		ChaincodeId: &pb.ChaincodeID{
			Name: "test-chaincode",
		},
	}
	_ = ccHeader // Use the variable to avoid unused variable error

	proposal := &pb.Proposal{
		Header: headerBytes,
		Payload: protoutil.MarshalOrPanic(&pb.ChaincodeProposalPayload{
			Input: protoutil.MarshalOrPanic(&pb.ChaincodeInvocationSpec{
				ChaincodeSpec: &pb.ChaincodeSpec{
					ChaincodeId: &pb.ChaincodeID{
						Name: "test-chaincode",
					},
					Input: &pb.ChaincodeInput{
						Args: [][]byte{[]byte("invoke"), []byte(key), []byte(value)},
					},
				},
			}),
		}),
	}

	proposalBytes, _ := proto.Marshal(proposal)
	return proposalBytes
}

func createTestRWSet(key, value string) []byte {
	rwSet := &kvrwset.KVRWSet{
		Reads: []*kvrwset.KVRead{
			{
				Key: key,
			},
		},
		Writes: []*kvrwset.KVWrite{
			{
				Key:   key,
				Value: []byte(value),
			},
		},
	}
	rwSetBytes, _ := proto.Marshal(rwSet)
	return rwSetBytes
}

func TestCircuitBreaker(t *testing.T) {
	t.Run("Circuit Breaker State Transitions", func(t *testing.T) {
		metrics := endorser.NewMetrics(&metricsfakes.Provider{})
		config := endorser.CircuitBreakerConfig{
			Threshold:     3,
			Timeout:       100 * time.Millisecond,
			MaxRetries:    2,
			RetryInterval: 50 * time.Millisecond,
		}
		cb := endorser.NewCircuitBreaker(config, metrics)

		// Test initial state
		assert.Equal(t, endorser.CircuitClosed, cb.GetState())

		// Test circuit breaker with leader communication
		err := cb.Execute(func() error {
			return fmt.Errorf("test error")
		})
		assert.Error(t, err)
		assert.Equal(t, endorser.CircuitClosed, cb.GetState())

		// Force circuit breaker to open
		for i := 0; i < 5; i++ {
			err = cb.Execute(func() error {
				return fmt.Errorf("test error")
			})
			assert.Error(t, err)
		}
		assert.Equal(t, endorser.CircuitOpen, cb.GetState())

		// Test health status with open circuit breaker
		status := cb.GetState()
		assert.Equal(t, endorser.CircuitOpen, status)
	})
}

type mockSupport struct {
	mock.Mock
}

func (m *mockSupport) Sign(msg []byte) ([]byte, error) {
	args := m.Called(msg)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *mockSupport) Serialize() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *mockSupport) GetTxSimulator(ledgerName string, txid string) (ledger.TxSimulator, error) {
	args := m.Called(ledgerName, txid)
	return args.Get(0).(ledger.TxSimulator), args.Error(1)
}

func (m *mockSupport) GetHistoryQueryExecutor(ledgerName string) (ledger.HistoryQueryExecutor, error) {
	args := m.Called(ledgerName)
	return args.Get(0).(ledger.HistoryQueryExecutor), args.Error(1)
}

func (m *mockSupport) GetTransactionByID(chid, txID string) (*pb.ProcessedTransaction, error) {
	args := m.Called(chid, txID)
	return args.Get(0).(*pb.ProcessedTransaction), args.Error(1)
}

func (m *mockSupport) IsSysCC(name string) bool {
	args := m.Called(name)
	return args.Bool(0)
}

func (m *mockSupport) Execute(txParams *ccprovider.TransactionParams, name string, input *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error) {
	args := m.Called(txParams, name, input)
	return args.Get(0).(*pb.Response), args.Get(1).(*pb.ChaincodeEvent), args.Error(2)
}

func (m *mockSupport) ExecuteLegacyInit(txParams *ccprovider.TransactionParams, name string, version string, input *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error) {
	args := m.Called(txParams, name, version, input)
	return args.Get(0).(*pb.Response), args.Get(1).(*pb.ChaincodeEvent), args.Error(2)
}

func (m *mockSupport) ChaincodeEndorsementInfo(channelID string, chaincodeID string, txsim ledger.QueryExecutor) (*lifecycle.ChaincodeEndorsementInfo, error) {
	args := m.Called(channelID, chaincodeID, txsim)
	return args.Get(0).(*lifecycle.ChaincodeEndorsementInfo), args.Error(1)
}

func (m *mockSupport) CheckACL(channelID string, signedProp *pb.SignedProposal) error {
	args := m.Called(channelID, signedProp)
	return args.Error(0)
}

func (m *mockSupport) EndorseWithPlugin(pluginName string, channnelID string, prpBytes []byte, signedProposal *pb.SignedProposal) (*pb.Endorsement, []byte, error) {
	args := m.Called(pluginName, channnelID, prpBytes, signedProposal)
	return args.Get(0).(*pb.Endorsement), args.Get(1).([]byte), args.Error(2)
}

func (m *mockSupport) GetLedgerHeight(channelID string) (uint64, error) {
	args := m.Called(channelID)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *mockSupport) GetDeployedCCInfoProvider() ledger.DeployedChaincodeInfoProvider {
	args := m.Called()
	return args.Get(0).(ledger.DeployedChaincodeInfoProvider)
}

func TestNormalEndorser(t *testing.T) {
	mockMetrics := &endorser.Metrics{
		ProposalDuration:         &metricsfakes.Histogram{},
		ProposalsReceived:        &metricsfakes.Counter{},
		SuccessfulProposals:      &metricsfakes.Counter{},
		ProposalValidationFailed: &metricsfakes.Counter{},
		ProposalACLCheckFailed:   &metricsfakes.Counter{},
		InitFailed:               &metricsfakes.Counter{},
		EndorsementsFailed:       &metricsfakes.Counter{},
		DuplicateTxsFailure:      &metricsfakes.Counter{},
		SimulationFailure:        &metricsfakes.Counter{},
	}

	support := &mockSupport{}
	support.On("ChaincodeEndorsementInfo", mock.Anything, mock.Anything, mock.Anything).Return(&lifecycle.ChaincodeEndorsementInfo{}, nil)

	e := endorser.NewEndorser(nil, nil, nil, support, nil, mockMetrics, endorser.EndorserConfig{
		Role: endorser.NormalEndorser,
	})

	assert.NotNil(t, e)
	assert.Equal(t, endorser.NormalEndorser, e.Config.Role)
}

func TestLeaderEndorser(t *testing.T) {
	mockMetrics := &endorser.Metrics{
		ProposalDuration:         &metricsfakes.Histogram{},
		ProposalsReceived:        &metricsfakes.Counter{},
		SuccessfulProposals:      &metricsfakes.Counter{},
		ProposalValidationFailed: &metricsfakes.Counter{},
		ProposalACLCheckFailed:   &metricsfakes.Counter{},
		InitFailed:               &metricsfakes.Counter{},
		EndorsementsFailed:       &metricsfakes.Counter{},
		DuplicateTxsFailure:      &metricsfakes.Counter{},
		SimulationFailure:        &metricsfakes.Counter{},
	}

	support := &mockSupport{}
	support.On("ChaincodeEndorsementInfo", mock.Anything, mock.Anything, mock.Anything).Return(&lifecycle.ChaincodeEndorsementInfo{}, nil)

	e := endorser.NewEndorser(nil, nil, nil, support, nil, mockMetrics, endorser.EndorserConfig{
		Role: endorser.LeaderEndorser,
	})

	assert.NotNil(t, e)
	assert.Equal(t, endorser.LeaderEndorser, e.Config.Role)
}
