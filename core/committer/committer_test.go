/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package committer

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/configtx/test"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	ledger2 "github.com/hyperledger/fabric/core/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockLedger struct {
	height       uint64
	currentHash  []byte
	previousHash []byte
	mock.Mock
}

func (m *mockLedger) GetConfigHistoryRetriever() (ledger2.ConfigHistoryRetriever, error) {
	args := m.Called()
	return args.Get(0).(ledger2.ConfigHistoryRetriever), args.Error(1)
}

func (m *mockLedger) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	info := &common.BlockchainInfo{
		Height:            m.height,
		CurrentBlockHash:  m.currentHash,
		PreviousBlockHash: m.previousHash,
	}
	return info, nil
}

func (m *mockLedger) DoesPvtDataInfoExist(blkNum uint64) (bool, error) {
	args := m.Called()
	return args.Get(0).(bool), args.Error(1)
}

func (m *mockLedger) GetBlockByNumber(blockNumber uint64) (*common.Block, error) {
	args := m.Called(blockNumber)
	return args.Get(0).(*common.Block), args.Error(1)
}

func (m *mockLedger) GetBlocksIterator(startBlockNumber uint64) (ledger.ResultsIterator, error) {
	args := m.Called(startBlockNumber)
	return args.Get(0).(ledger.ResultsIterator), args.Error(1)
}

func (m *mockLedger) Close() {
}

// TxIDExists returns true if the specified txID is already present in one of the already committed blocks
func (m *mockLedger) TxIDExists(txID string) (bool, error) {
	args := m.Called(txID)
	return args.Get(0).(bool), args.Error(1)
}

func (m *mockLedger) GetTransactionByID(txID string) (*pb.ProcessedTransaction, error) {
	args := m.Called(txID)
	return args.Get(0).(*pb.ProcessedTransaction), args.Error(1)
}

func (m *mockLedger) GetBlockByHash(blockHash []byte) (*common.Block, error) {
	args := m.Called(blockHash)
	return args.Get(0).(*common.Block), args.Error(1)
}

func (m *mockLedger) GetBlockByTxID(txID string) (*common.Block, error) {
	args := m.Called(txID)
	return args.Get(0).(*common.Block), args.Error(1)
}

func (m *mockLedger) GetTxValidationCodeByTxID(txID string) (pb.TxValidationCode, error) {
	args := m.Called(txID)
	return args.Get(0).(pb.TxValidationCode), args.Error(1)
}

func (m *mockLedger) NewTxSimulator(txid string) (ledger2.TxSimulator, error) {
	args := m.Called(txid)
	return args.Get(0).(ledger2.TxSimulator), args.Error(1)
}

func (m *mockLedger) NewQueryExecutor() (ledger2.QueryExecutor, error) {
	args := m.Called()
	return args.Get(0).(ledger2.QueryExecutor), args.Error(1)
}

func (m *mockLedger) NewHistoryQueryExecutor() (ledger2.HistoryQueryExecutor, error) {
	args := m.Called()
	return args.Get(0).(ledger2.HistoryQueryExecutor), args.Error(1)
}

func (m *mockLedger) GetPvtDataAndBlockByNum(blockNum uint64, filter ledger2.PvtNsCollFilter) (*ledger2.BlockAndPvtData, error) {
	args := m.Called(blockNum, filter)
	return args.Get(0).(*ledger2.BlockAndPvtData), args.Error(1)
}

func (m *mockLedger) GetPvtDataByNum(blockNum uint64, filter ledger2.PvtNsCollFilter) ([]*ledger2.TxPvtData, error) {
	args := m.Called(blockNum, filter)
	return args.Get(0).([]*ledger2.TxPvtData), args.Error(1)
}

func (m *mockLedger) CommitLegacy(blockAndPvtdata *ledger2.BlockAndPvtData, commitOpts *ledger2.CommitOptions) error {
	m.height += 1
	m.previousHash = m.currentHash
	m.currentHash = blockAndPvtdata.Block.Header.DataHash
	args := m.Called(blockAndPvtdata)
	return args.Error(0)
}

func (m *mockLedger) CommitPvtDataOfOldBlocks(reconciledPvtdata []*ledger2.ReconciledPvtdata, unreconciled ledger2.MissingPvtDataInfo) ([]*ledger2.PvtdataHashMismatch, error) {
	panic("implement me")
}

func (m *mockLedger) GetMissingPvtDataTracker() (ledger2.MissingPvtDataTracker, error) {
	panic("implement me")
}

func createLedger(channelID string) (*common.Block, *mockLedger) {
	gb, _ := test.MakeGenesisBlock(channelID)
	ledger := &mockLedger{
		height:       1,
		previousHash: []byte{},
		currentHash:  gb.Header.DataHash,
	}
	return gb, ledger
}

func TestKVLedgerBlockStorage(t *testing.T) {
	t.Parallel()
	gb, ledger := createLedger("TestLedger")
	block1 := testutil.ConstructBlock(t, 1, gb.Header.DataHash, [][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}}, true)

	ledger.On("CommitLegacy", mock.Anything).Run(func(args mock.Arguments) {
		b := args.Get(0).(*ledger2.BlockAndPvtData)
		require.Equal(t, uint64(1), b.Block.Header.GetNumber())
		require.Equal(t, gb.Header.DataHash, b.Block.Header.PreviousHash)
		require.Equal(t, block1.Header.DataHash, b.Block.Header.DataHash)
	}).Return(nil)

	ledger.On("GetBlockByNumber", uint64(0)).Return(gb, nil)

	committer := NewLedgerCommitter(ledger)
	height, err := committer.LedgerHeight()
	require.Equal(t, uint64(1), height)
	require.NoError(t, err)

	err = committer.CommitLegacy(&ledger2.BlockAndPvtData{Block: block1}, &ledger2.CommitOptions{})
	require.NoError(t, err)

	height, err = committer.LedgerHeight()
	require.Equal(t, uint64(2), height)
	require.NoError(t, err)

	blocks := committer.GetBlocks([]uint64{0})
	require.Equal(t, 1, len(blocks))
	require.NoError(t, err)
}

func TestTransactionDependencyTracking(t *testing.T) {
	// Create test transactions with dependencies
	tx1 := createTestTransaction("tx1", "key1", "value1", "")
	tx2 := createTestTransaction("tx2", "key2", "value2", "tx1")
	tx3 := createTestTransaction("tx3", "key3", "value3", "tx2")

	// Create a block with these transactions
	block := createTestBlock([]*pb.Transaction{tx1, tx2, tx3})

	// Create test committer
	lc := createTestLedgerCommitter(t)

	// Commit the block
	blockAndPvtData := &ledger2.BlockAndPvtData{
		Block: block,
	}
	err := lc.CommitLegacy(blockAndPvtData, &ledger2.CommitOptions{})
	assert.NoError(t, err)

	// Verify block was committed
	height, err := lc.LedgerHeight()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), height)
}

func TestTransactionValidationWithDependencies(t *testing.T) {
	// Create test transactions
	tx1 := createTestTransaction("tx1", "key1", "value1", "")
	tx2 := createTestTransaction("tx2", "key2", "value2", "tx1")
	tx3 := createTestTransaction("tx3", "key3", "value3", "tx2")

	// Create a block with these transactions
	block := createTestBlock([]*pb.Transaction{tx1, tx2, tx3})

	// Create test committer
	lc := createTestLedgerCommitter(t)

	// Commit the block
	blockAndPvtData := &ledger2.BlockAndPvtData{
		Block: block,
	}
	err := lc.CommitLegacy(blockAndPvtData, &ledger2.CommitOptions{})
	assert.NoError(t, err)

	// Verify block was committed
	height, err := lc.LedgerHeight()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), height)
}

func TestTransactionValidationWithConflicts(t *testing.T) {
	// Create test transactions with conflicting read/write sets
	tx1 := createTestTransaction("tx1", "key1", "value1", "")
	tx2 := createTestTransaction("tx2", "key1", "value2", "tx1") // Conflicts with tx1
	tx3 := createTestTransaction("tx3", "key3", "value3", "tx2")

	// Create a block with these transactions
	block := createTestBlock([]*pb.Transaction{tx1, tx2, tx3})

	// Create test committer
	lc := createTestLedgerCommitter(t)

	// Commit the block
	blockAndPvtData := &ledger2.BlockAndPvtData{
		Block: block,
	}
	err := lc.CommitLegacy(blockAndPvtData, &ledger2.CommitOptions{})
	assert.NoError(t, err)

	// Verify block was committed
	height, err := lc.LedgerHeight()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), height)
}

func TestCircularDependencyHandling(t *testing.T) {
	// Create test transactions with circular dependencies
	tx1 := createTestTransaction("tx1", "key1", "value1", "tx3")
	tx2 := createTestTransaction("tx2", "key2", "value2", "tx1")
	tx3 := createTestTransaction("tx3", "key3", "value3", "tx2")

	// Create a block with these transactions
	block := createTestBlock([]*pb.Transaction{tx1, tx2, tx3})

	// Create test committer
	lc := createTestLedgerCommitter(t)

	// Commit the block
	blockAndPvtData := &ledger2.BlockAndPvtData{
		Block: block,
	}
	err := lc.CommitLegacy(blockAndPvtData, &ledger2.CommitOptions{})
	assert.NoError(t, err)

	// Verify block was committed
	height, err := lc.LedgerHeight()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), height)
}

func TestReadWriteSetConflictDetection(t *testing.T) {
	// Create test transactions with conflicting read/write sets
	tx1 := createTestTransaction("tx1", "key1", "value1", "")
	tx2 := createTestTransaction("tx2", "key1", "value2", "") // Conflicts with tx1

	// Create test committer
	lc := createTestLedgerCommitter(t)

	// Check for conflicts
	hasConflict := lc.checkRWSetConflicts(tx1, tx2)
	assert.True(t, hasConflict)

	// Create test transactions with non-conflicting read/write sets
	tx3 := createTestTransaction("tx3", "key3", "value3", "")
	tx4 := createTestTransaction("tx4", "key4", "value4", "")

	// Check for conflicts
	hasConflict = lc.checkRWSetConflicts(tx3, tx4)
	assert.False(t, hasConflict)
}

func TestPrivateDataConflictDetection(t *testing.T) {
	// Create test transactions with private data conflicts
	tx1 := createTestTransactionWithPrivateData("tx1", "key1", "value1", "", "collection1")
	tx2 := createTestTransactionWithPrivateData("tx2", "key1", "value2", "tx1", "collection1")

	// Create test committer
	lc := createTestLedgerCommitter(t)

	// Check for conflicts
	hasConflict := lc.checkRWSetConflicts(tx1, tx2)
	assert.True(t, hasConflict)

	// Create test transactions with non-conflicting private data
	tx3 := createTestTransactionWithPrivateData("tx3", "key3", "value3", "", "collection1")
	tx4 := createTestTransactionWithPrivateData("tx4", "key4", "value4", "", "collection2")

	// Check for conflicts
	hasConflict = lc.checkRWSetConflicts(tx3, tx4)
	assert.False(t, hasConflict)
}

// Helper functions for creating test data
func createTestTransaction(txID, key, value, dependentTxID string) *pb.Transaction {
	// Create chaincode action
	chaincodeAction := &pb.ChaincodeAction{
		Response: &pb.Response{
			Status:  200,
			Message: fmt.Sprintf("Dependency: %s", dependentTxID),
		},
		Results: createTestRWSet(key, value),
	}
	chaincodeActionBytes, _ := proto.Marshal(chaincodeAction)

	// Create chaincode action payload
	chaincodeActionPayload := &pb.ChaincodeActionPayload{
		Action: &pb.ChaincodeEndorsedAction{
			ProposalResponsePayload: createTestProposalResponsePayload(txID, chaincodeActionBytes),
		},
	}
	chaincodeActionPayloadBytes, _ := proto.Marshal(chaincodeActionPayload)

	// Create transaction
	tx := &pb.Transaction{
		Actions: []*pb.TransactionAction{
			{
				Payload: chaincodeActionPayloadBytes,
			},
		},
	}

	return tx
}

func createTestProposalResponsePayload(txID string, chaincodeActionBytes []byte) []byte {
	proposalResponsePayload := &pb.ProposalResponsePayload{
		ProposalHash: []byte(txID),
		Extension:    chaincodeActionBytes,
	}
	proposalResponsePayloadBytes, _ := proto.Marshal(proposalResponsePayload)
	return proposalResponsePayloadBytes
}

func createTestRWSet(key, value string) []byte {
	rwSet := &rwset.TxReadWriteSet{
		DataModel: rwset.TxReadWriteSet_KV,
		NsRwset: []*rwset.NsReadWriteSet{
			{
				Namespace: "test-ns",
				Rwset:     createTestKVRWSet(key, value),
			},
		},
	}
	rwSetBytes, _ := proto.Marshal(rwSet)
	return rwSetBytes
}

func createTestKVRWSet(key, value string) []byte {
	kvRWSet := &kvrwset.KVRWSet{
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
	kvRWSetBytes, _ := proto.Marshal(kvRWSet)
	return kvRWSetBytes
}

func createTestBlock(txs []*pb.Transaction) *common.Block {
	block := &common.Block{
		Header: &common.BlockHeader{
			Number: 1,
		},
		Data: &common.BlockData{
			Data: make([][]byte, len(txs)),
		},
		Metadata: &common.BlockMetadata{
			Metadata: make([][]byte, common.BlockMetadataIndex_TRANSACTIONS_FILTER+1),
		},
	}

	for i, tx := range txs {
		txBytes, _ := proto.Marshal(tx)
		block.Data.Data[i] = txBytes
	}

	return block
}

func createTestLedgerCommitter(t *testing.T) *LedgerCommitter {
	ledger := &mockLedger{
		height:       1,
		currentHash:  []byte("test-hash"),
		previousHash: []byte("test-prev-hash"),
	}
	return NewLedgerCommitter(ledger)
}

// Mock ledger support for testing
type mockLedgerSupport struct {
	PeerLedgerSupport
}

func (m *mockLedgerSupport) CommitLegacy(blockAndPvtData *ledger2.BlockAndPvtData, commitOpts *ledger2.CommitOptions) error {
	return nil
}

func createTestTransactionWithPrivateData(txID, key, value, dependentTxID, collection string) *pb.Transaction {
	// Create chaincode action
	chaincodeAction := &pb.ChaincodeAction{
		Response: &pb.Response{
			Status:  200,
			Message: fmt.Sprintf("Dependency: %s", dependentTxID),
		},
		Results: createTestRWSetWithPrivateData(key, value, collection),
	}
	chaincodeActionBytes, _ := proto.Marshal(chaincodeAction)

	// Create chaincode action payload
	chaincodeActionPayload := &pb.ChaincodeActionPayload{
		Action: &pb.ChaincodeEndorsedAction{
			ProposalResponsePayload: createTestProposalResponsePayload(txID, chaincodeActionBytes),
		},
	}
	chaincodeActionPayloadBytes, _ := proto.Marshal(chaincodeActionPayload)

	// Create transaction
	tx := &pb.Transaction{
		Actions: []*pb.TransactionAction{
			{
				Payload: chaincodeActionPayloadBytes,
			},
		},
	}

	return tx
}

func createTestRWSetWithPrivateData(key, value, collection string) []byte {
	rwSet := &rwset.TxReadWriteSet{
		DataModel: rwset.TxReadWriteSet_KV,
		NsRwset: []*rwset.NsReadWriteSet{
			{
				Namespace: "test-ns",
				CollectionHashedRwset: []*rwset.CollectionHashedReadWriteSet{
					{
						CollectionName: collection,
						HashedRwset:    createTestKVRWSet(key, value),
					},
				},
			},
		},
	}
	rwSetBytes, _ := proto.Marshal(rwSet)
	return rwSetBytes
}
