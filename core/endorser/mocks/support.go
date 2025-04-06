package mocks

import (
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/stretchr/testify/mock"
)

type Support struct {
	mock.Mock
}

func (m *Support) Sign(msg []byte) ([]byte, error) {
	args := m.Called(msg)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *Support) Serialize() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *Support) GetTxSimulator(ledgerName string, txid string) (ledger.TxSimulator, error) {
	args := m.Called(ledgerName, txid)
	return args.Get(0).(ledger.TxSimulator), args.Error(1)
}

func (m *Support) GetHistoryQueryExecutor(ledgerName string) (ledger.HistoryQueryExecutor, error) {
	args := m.Called(ledgerName)
	return args.Get(0).(ledger.HistoryQueryExecutor), args.Error(1)
}

func (m *Support) GetTransactionByID(chid, txID string) (*peer.ProcessedTransaction, error) {
	args := m.Called(chid, txID)
	return args.Get(0).(*peer.ProcessedTransaction), args.Error(1)
}

func (m *Support) IsSysCC(name string) bool {
	args := m.Called(name)
	return args.Bool(0)
}

func (m *Support) Execute(txParams *ccprovider.TransactionParams, name string, input *peer.ChaincodeInput) (*peer.Response, *peer.ChaincodeEvent, error) {
	args := m.Called(txParams, name, input)
	return args.Get(0).(*peer.Response), args.Get(1).(*peer.ChaincodeEvent), args.Error(2)
}

func (m *Support) ExecuteLegacyInit(txParams *ccprovider.TransactionParams, name, version string, input *peer.ChaincodeInput) (*peer.Response, *peer.ChaincodeEvent, error) {
	args := m.Called(txParams, name, version, input)
	return args.Get(0).(*peer.Response), args.Get(1).(*peer.ChaincodeEvent), args.Error(2)
}

func (m *Support) ChaincodeEndorsementInfo(channelID, chaincodeID string, txsim ledger.QueryExecutor) (*lifecycle.ChaincodeEndorsementInfo, error) {
	args := m.Called(channelID, chaincodeID, txsim)
	return args.Get(0).(*lifecycle.ChaincodeEndorsementInfo), args.Error(1)
}

func (m *Support) CheckACL(channelID string, signedProp *peer.SignedProposal) error {
	args := m.Called(channelID, signedProp)
	return args.Error(0)
}

func (m *Support) EndorseWithPlugin(pluginName, channnelID string, prpBytes []byte, signedProposal *peer.SignedProposal) (*peer.Endorsement, []byte, error) {
	args := m.Called(pluginName, channnelID, prpBytes, signedProposal)
	return args.Get(0).(*peer.Endorsement), args.Get(1).([]byte), args.Error(2)
}

func (m *Support) GetLedgerHeight(channelID string) (uint64, error) {
	args := m.Called(channelID)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *Support) GetDeployedCCInfoProvider() ledger.DeployedChaincodeInfoProvider {
	args := m.Called()
	return args.Get(0).(ledger.DeployedChaincodeInfoProvider)
}
