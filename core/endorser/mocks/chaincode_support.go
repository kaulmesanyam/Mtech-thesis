package mocks

import (
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/endorser"
	"github.com/stretchr/testify/mock"
)

type ChaincodeSupport struct {
	mock.Mock
}

func (cs *ChaincodeSupport) Execute(txParams *ccprovider.TransactionParams, chaincodeName string, input *peer.ChaincodeInput) (*peer.Response, *peer.ChaincodeEvent, error) {
	args := cs.Called(txParams, chaincodeName, input)
	return args.Get(0).(*peer.Response), args.Get(1).(*peer.ChaincodeEvent), args.Error(2)
}

func (cs *ChaincodeSupport) ExecuteLegacyInit(txParams *ccprovider.TransactionParams, chaincodeName string, chaincodeVersion string, input *peer.ChaincodeInput) (*peer.Response, *peer.ChaincodeEvent, error) {
	args := cs.Called(txParams, chaincodeName, chaincodeVersion, input)
	return args.Get(0).(*peer.Response), args.Get(1).(*peer.ChaincodeEvent), args.Error(2)
}

func (cs *ChaincodeSupport) Channel(channelID string) *endorser.Channel {
	args := cs.Called(channelID)
	return args.Get(0).(*endorser.Channel)
}
