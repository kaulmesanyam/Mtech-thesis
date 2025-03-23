/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package committer

import (
	// "fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("committer")

// TransactionDependency represents a dependency between transactions
type TransactionDependency struct {
	TxID           string
	DependentTxIDs []string
	HasDependency  bool
}

// TransactionDAG represents a Directed Acyclic Graph of transaction dependencies
type TransactionDAG struct {
	Nodes map[string]*TransactionDependency
	// Map of transaction to its dependent transactions (transactions that depend on this one)
	Dependencies map[string][]string
	// Track the level in the DAG for each transaction (for parallel processing)
	Levels map[string]int
	// Track transaction validation result
	ValidationResults map[string]bool
	// Map of transaction IDs to their index in the block
	TxIndices map[string]int
	// Mutex for thread safety
	mutex sync.RWMutex
}

// NewTransactionDAG creates a new DAG for transaction dependencies
func NewTransactionDAG() *TransactionDAG {
	return &TransactionDAG{
		Nodes:             make(map[string]*TransactionDependency),
		Dependencies:      make(map[string][]string),
		Levels:            make(map[string]int),
		ValidationResults: make(map[string]bool),
		TxIndices:         make(map[string]int),
	}
}

// AddTransaction adds a transaction to the DAG
func (dag *TransactionDAG) AddTransaction(txID string, txIndex int, hasDependency bool, dependentTxID string) {
	dag.mutex.Lock()
	defer dag.mutex.Unlock()

	// Store the transaction index
	dag.TxIndices[txID] = txIndex

	// Create a new node if it doesn't exist
	if _, exists := dag.Nodes[txID]; !exists {
		dag.Nodes[txID] = &TransactionDependency{
			TxID:           txID,
			DependentTxIDs: []string{},
			HasDependency:  hasDependency,
		}
	}

	// If this transaction has a dependency, add the relationship
	if hasDependency && dependentTxID != "" {
		// Add dependentTxID to the list of dependencies
		txDep := dag.Nodes[txID]
		txDep.DependentTxIDs = append(txDep.DependentTxIDs, dependentTxID)

		// Update the reverse dependency map
		if _, exists := dag.Dependencies[dependentTxID]; !exists {
			dag.Dependencies[dependentTxID] = []string{}
		}
		dag.Dependencies[dependentTxID] = append(dag.Dependencies[dependentTxID], txID)
	}
}

// CalculateLevels determines the level of each transaction in the DAG
// Level 0 transactions have no dependencies
// Higher levels depend on lower levels
func (dag *TransactionDAG) CalculateLevels() {
	dag.mutex.Lock()
	defer dag.mutex.Unlock()

	// First, reset all levels
	dag.Levels = make(map[string]int)

	// First pass: Set all transactions with no dependencies to level 0
	for txID, node := range dag.Nodes {
		if !node.HasDependency || len(node.DependentTxIDs) == 0 {
			dag.Levels[txID] = 0
		}
	}

	// Keep processing until all transactions have been assigned a level
	changed := true
	for changed {
		changed = false
		for txID, node := range dag.Nodes {
			// Skip transactions that already have a level assigned
			if _, hasLevel := dag.Levels[txID]; hasLevel {
				continue
			}

			// Check if all dependencies have levels assigned
			allDepsHaveLevel := true
			maxDepLevel := -1

			for _, depTxID := range node.DependentTxIDs {
				if level, exists := dag.Levels[depTxID]; exists {
					if level > maxDepLevel {
						maxDepLevel = level
					}
				} else {
					allDepsHaveLevel = false
					break
				}
			}

			// If all dependencies have levels, set this transaction's level to max + 1
			if allDepsHaveLevel {
				dag.Levels[txID] = maxDepLevel + 1
				changed = true
			}
		}
	}

	// Log any transactions that couldn't be assigned a level (circular dependencies)
	for txID := range dag.Nodes {
		if _, hasLevel := dag.Levels[txID]; !hasLevel {
			logger.Warningf("Transaction %s has circular dependencies, setting to level 0", txID)
			dag.Levels[txID] = 0
		}
	}
}

// GetTransactionsByLevel returns transactions grouped by their level in the DAG
func (dag *TransactionDAG) GetTransactionsByLevel() map[int][]string {
	dag.mutex.RLock()
	defer dag.mutex.RUnlock()

	levelMap := make(map[int][]string)

	for txID, level := range dag.Levels {
		if _, exists := levelMap[level]; !exists {
			levelMap[level] = []string{}
		}
		levelMap[level] = append(levelMap[level], txID)
	}

	return levelMap
}

// SetValidationResult sets the validation result for a transaction
func (dag *TransactionDAG) SetValidationResult(txID string, isValid bool) {
	dag.mutex.Lock()
	defer dag.mutex.Unlock()

	dag.ValidationResults[txID] = isValid
}

// IsValid returns whether a transaction is valid
func (dag *TransactionDAG) IsValid(txID string) bool {
	dag.mutex.RLock()
	defer dag.mutex.RUnlock()

	if result, exists := dag.ValidationResults[txID]; exists {
		return result
	}
	return false
}

// GetIndexByTxID returns the block index for a transaction ID
func (dag *TransactionDAG) GetIndexByTxID(txID string) (int, bool) {
	dag.mutex.RLock()
	defer dag.mutex.RUnlock()

	index, exists := dag.TxIndices[txID]
	return index, exists
}

// BuildDAGFromBlock constructs a DAG for the block by extracting dependency information from transactions
func BuildDAGFromBlock(block *common.Block) (*TransactionDAG, error) {
	dag := NewTransactionDAG()

	// Extract envelope from each transaction
	for i := 0; i < len(block.Data.Data); i++ {
		txEnvelopeBytes := block.Data.Data[i]

		// Extract transaction envelope from block
		env, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			logger.Warningf("Failed to get envelope from block for tx %d: %s", i, err)
			continue
		}

		// Extract the payload from the envelope
		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			logger.Warningf("Failed to unmarshal payload for tx %d: %s", i, err)
			continue
		}

		// Extract the channel header to get the transaction ID
		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			logger.Warningf("Failed to unmarshal channel header for tx %d: %s", i, err)
			continue
		}

		txID := chdr.TxId

		// Extract the transaction
		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			logger.Warningf("Failed to unmarshal transaction for tx %d: %s", i, err)
			continue
		}

		// Extract dependency information from transaction actions
		hasDependency := false
		dependentTxID := ""

		for _, action := range tx.Actions {
			chaincodeAction := &peer.ChaincodeAction{}
			if err := proto.Unmarshal(action.Payload, chaincodeAction); err != nil {
				logger.Warningf("Failed to unmarshal chaincode action for tx %s: %s", txID, err)
				continue
			}

			// Extract dependency information from the response message
			if chaincodeAction.Response != nil && chaincodeAction.Response.Message != "" {
				hasDependency, dependentTxID, _, err = ParseDependencyInfo(chaincodeAction.Response.Message)
				if err != nil {
					logger.Warningf("Failed to parse dependency info for tx %s: %s", txID, err)
					continue
				}
				if hasDependency {
					break
				}
			}
		}

		// Add transaction to DAG
		dag.AddTransaction(txID, i, hasDependency, dependentTxID)
	}

	// Calculate levels for parallel processing
	dag.CalculateLevels()

	return dag, nil
}

// ParseDependencyInfo parses the dependency info from the response message
func ParseDependencyInfo(responseMsg string) (bool, string, int64, error) {
	// Example format: "DependencyInfo:HasDependency=true,DependentTxID=tx123,ExpiryTime=1234567"
	if !strings.Contains(responseMsg, "DependencyInfo:") {
		return false, "", 0, nil
	}

	parts := strings.Split(responseMsg, ":")
	if len(parts) < 2 {
		return false, "", 0, errors.New("invalid dependency info format")
	}

	info := parts[1]
	infoMap := make(map[string]string)

	items := strings.Split(info, ",")
	for _, item := range items {
		kv := strings.Split(item, "=")
		if len(kv) == 2 {
			infoMap[kv[0]] = kv[1]
		}
	}

	hasDependency := false
	if val, ok := infoMap["HasDependency"]; ok {
		hasDependency, _ = strconv.ParseBool(val)
	}

	dependentTxID := ""
	if val, ok := infoMap["DependentTxID"]; ok {
		dependentTxID = val
	}

	expiryTime := int64(0)
	if val, ok := infoMap["ExpiryTime"]; ok {
		expiryTime, _ = strconv.ParseInt(val, 10, 64)
	}

	return hasDependency, dependentTxID, expiryTime, nil
}

//--------!!!IMPORTANT!!-!!IMPORTANT!!-!!IMPORTANT!!---------
// This is used merely to complete the loop for the "skeleton"
// path so we can reason about and modify committer component
// more effectively using code.

// PeerLedgerSupport abstract out the API's of ledger.PeerLedger interface
// required to implement LedgerCommitter
type PeerLedgerSupport interface {
	GetPvtDataAndBlockByNum(blockNum uint64, filter ledger.PvtNsCollFilter) (*ledger.BlockAndPvtData, error)

	GetPvtDataByNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error)

	CommitLegacy(blockAndPvtdata *ledger.BlockAndPvtData, commitOpts *ledger.CommitOptions) error

	CommitPvtDataOfOldBlocks(reconciledPvtdata []*ledger.ReconciledPvtdata, unreconciled ledger.MissingPvtDataInfo) ([]*ledger.PvtdataHashMismatch, error)

	GetBlockchainInfo() (*common.BlockchainInfo, error)

	DoesPvtDataInfoExist(blockNum uint64) (bool, error)

	GetBlockByNumber(blockNumber uint64) (*common.Block, error)

	GetConfigHistoryRetriever() (ledger.ConfigHistoryRetriever, error)

	GetMissingPvtDataTracker() (ledger.MissingPvtDataTracker, error)

	Close()
}

// LedgerCommitter is the implementation of Committer interface
// it keeps the reference to the ledger to commit blocks and retrieve
// chain information
type LedgerCommitter struct {
	PeerLedgerSupport
}

// NewLedgerCommitter is a factory function to create an instance of the committer
// which passes incoming blocks via validation and commits them into the ledger.
func NewLedgerCommitter(ledger PeerLedgerSupport) *LedgerCommitter {
	return &LedgerCommitter{PeerLedgerSupport: ledger}
}

// CommitLegacy commits blocks atomically with private data
func (lc *LedgerCommitter) CommitLegacy(blockAndPvtData *ledger.BlockAndPvtData, commitOpts *ledger.CommitOptions) error {
	block := blockAndPvtData.Block

	// 1. Construct a DAG for the block
	dag, err := BuildDAGFromBlock(block)
	if err != nil {
		logger.Errorf("Failed to build DAG for block %d: %s", block.Header.Number, err)
		// Continue with normal processing if DAG building fails
		return lc.legacyCommit(blockAndPvtData, commitOpts)
	}

	logger.Infof("Successfully built DAG for block %d with %d transactions",
		block.Header.Number, len(dag.Nodes))

	// 2. Process transactions according to the DAG
	err = lc.processBlockWithDAG(blockAndPvtData, commitOpts, dag)
	if err != nil {
		logger.Errorf("Failed to process block with DAG: %s", err)
		// Fall back to legacy commit if DAG processing fails
		return lc.legacyCommit(blockAndPvtData, commitOpts)
	}

	return nil
}

// legacyCommit is the original commit function without DAG processing
func (lc *LedgerCommitter) legacyCommit(blockAndPvtData *ledger.BlockAndPvtData, commitOpts *ledger.CommitOptions) error {
	// Committing new block
	if err := lc.PeerLedgerSupport.CommitLegacy(blockAndPvtData, commitOpts); err != nil {
		return err
	}

	return nil
}

// processBlockWithDAG processes a block using the transaction dependency DAG
func (lc *LedgerCommitter) processBlockWithDAG(blockAndPvtData *ledger.BlockAndPvtData,
	commitOpts *ledger.CommitOptions, dag *TransactionDAG) error {

	// Get transactions by level for parallel processing
	txsByLevel := dag.GetTransactionsByLevel()
	maxLevel := -1

	// Find the max level
	for level := range txsByLevel {
		if level > maxLevel {
			maxLevel = level
		}
	}

	logger.Infof("Processing block with DAG: %d levels of transactions", maxLevel+1)

	// Process each level in order (level 0 first, then 1, etc.)
	for level := 0; level <= maxLevel; level++ {
		txs, exists := txsByLevel[level]
		if !exists {
			continue
		}

		logger.Debugf("Processing %d transactions at level %d", len(txs), level)

		// Process transactions at this level in parallel
		var wg sync.WaitGroup
		var mutex sync.Mutex
		txValidationResults := make(map[string]bool)

		for _, txID := range txs {
			// Check if dependencies are valid (if any)
			if level > 0 {
				// For transactions with dependencies, check if dependencies were valid
				node := dag.Nodes[txID]
				allDepsValid := true

				for _, depTxID := range node.DependentTxIDs {
					if !dag.IsValid(depTxID) {
						// One of the dependencies is invalid, so this transaction is also invalid
						allDepsValid = false
						logger.Infof("Transaction %s marked as invalid because dependency %s is invalid",
							txID, depTxID)
						break
					}
				}

				if !allDepsValid {
					// Mark this transaction as invalid and skip processing
					dag.SetValidationResult(txID, false)
					txValidationResults[txID] = false
					continue
				}
			}

			// Process the transaction
			wg.Add(1)
			go func(id string) {
				defer wg.Done()

				// Get the transaction index in the block
				txIndex, exists := dag.GetIndexByTxID(id)
				if !exists {
					logger.Warningf("Transaction %s not found in index map", id)
					return
				}

				// Extract the transaction envelope
				txEnvelopeBytes := blockAndPvtData.Block.Data.Data[txIndex]
				env, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
				if err != nil {
					logger.Errorf("Failed to get envelope for tx %s: %s", id, err)
					mutex.Lock()
					txValidationResults[id] = false
					mutex.Unlock()
					dag.SetValidationResult(id, false)
					return
				}

				// Extract the payload
				payload, err := protoutil.UnmarshalPayload(env.Payload)
				if err != nil {
					logger.Errorf("Failed to unmarshal payload for tx %s: %s", id, err)
					mutex.Lock()
					txValidationResults[id] = false
					mutex.Unlock()
					dag.SetValidationResult(id, false)
					return
				}

				// Extract the transaction
				tx, err := protoutil.UnmarshalTransaction(payload.Data)
				if err != nil {
					logger.Errorf("Failed to unmarshal transaction for tx %s: %s", id, err)
					mutex.Lock()
					txValidationResults[id] = false
					mutex.Unlock()
					dag.SetValidationResult(id, false)
					return
				}

				// Validate chaincode actions
				isValid := true
				for _, action := range tx.Actions {
					chaincodeAction := &peer.ChaincodeAction{}
					if err := proto.Unmarshal(action.Payload, chaincodeAction); err != nil {
						logger.Errorf("Failed to unmarshal chaincode action for tx %s: %s", id, err)
						isValid = false
						break
					}

					// Check chaincode response status
					if chaincodeAction.Response == nil || chaincodeAction.Response.Status != 200 {
						logger.Errorf("Chaincode action failed for tx %s with status %d", id,
							chaincodeAction.Response.GetStatus())
						isValid = false
						break
					}
				}

				// Check for read/write set conflicts with dependencies
				if level > 0 && isValid {
					node := dag.Nodes[id]
					for _, depTxID := range node.DependentTxIDs {
						// Get the dependent transaction
						depTxIndex, exists := dag.GetIndexByTxID(depTxID)
						if !exists {
							continue
						}

						// Extract the dependent transaction
						depTxEnvelopeBytes := blockAndPvtData.Block.Data.Data[depTxIndex]
						depEnv, err := protoutil.GetEnvelopeFromBlock(depTxEnvelopeBytes)
						if err != nil {
							continue
						}

						depPayload, err := protoutil.UnmarshalPayload(depEnv.Payload)
						if err != nil {
							continue
						}

						depTx, err := protoutil.UnmarshalTransaction(depPayload.Data)
						if err != nil {
							continue
						}

						// Check for read/write set conflicts
						if lc.checkRWSetConflicts(tx, depTx) {
							logger.Infof("Transaction %s marked as invalid due to read/write set conflict with dependency %s",
								id, depTxID)
							isValid = false
							break
						}
					}
				}

				// Set the result
				mutex.Lock()
				txValidationResults[id] = isValid
				mutex.Unlock()

				// Update the DAG
				dag.SetValidationResult(id, isValid)

				logger.Debugf("Transaction %s (index %d) processed and marked as %v",
					id, txIndex, isValid)
			}(txID)
		}

		// Wait for all transactions at this level to be processed
		wg.Wait()
		logger.Debugf("Completed processing of level %d", level)
	}

	// After DAG-based processing, update the transaction validation flags in the block
	metadata := blockAndPvtData.Block.Metadata
	if len(metadata.Metadata) <= int(common.BlockMetadataIndex_TRANSACTIONS_FILTER) {
		metadata.Metadata = append(metadata.Metadata, []byte{})
	}

	txFilter := make([]uint8, len(blockAndPvtData.Block.Data.Data))
	for i := 0; i < len(txFilter); i++ {
		txFilter[i] = uint8(peer.TxValidationCode_VALID)
	}

	// Update validation flags based on our DAG processing results
	for txID := range dag.Nodes {
		txIndex, exists := dag.GetIndexByTxID(txID)
		if !exists {
			continue
		}

		if !dag.IsValid(txID) {
			// Mark as invalid with appropriate validation code
			txFilter[txIndex] = uint8(peer.TxValidationCode_MVCC_READ_CONFLICT)
		}
	}

	// Update the block metadata with our modified validation flags
	metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txFilter

	// Now commit the block with the updated validation flags
	return lc.PeerLedgerSupport.CommitLegacy(blockAndPvtData, commitOpts)
}

// GetPvtDataAndBlockByNum retrieves private data and block for given sequence number
func (lc *LedgerCommitter) GetPvtDataAndBlockByNum(seqNum uint64) (*ledger.BlockAndPvtData, error) {
	return lc.PeerLedgerSupport.GetPvtDataAndBlockByNum(seqNum, nil)
}

// LedgerHeight returns recently committed block sequence number
func (lc *LedgerCommitter) LedgerHeight() (uint64, error) {
	info, err := lc.GetBlockchainInfo()
	if err != nil {
		logger.Errorf("Cannot get blockchain info, %s", info)
		return 0, err
	}

	return info.Height, nil
}

// DoesPvtDataInfoExistInLedger returns true if the ledger has pvtdata info
// about a given block number.
func (lc *LedgerCommitter) DoesPvtDataInfoExistInLedger(blockNum uint64) (bool, error) {
	return lc.DoesPvtDataInfoExist(blockNum)
}

// GetBlocks used to retrieve blocks with sequence numbers provided in the slice
func (lc *LedgerCommitter) GetBlocks(blockSeqs []uint64) []*common.Block {
	var blocks []*common.Block

	for _, seqNum := range blockSeqs {
		if blck, err := lc.GetBlockByNumber(seqNum); err != nil {
			logger.Errorf("Not able to acquire block num %d, from the ledger skipping...", seqNum)
			continue
		} else {
			logger.Debug("Appending next block with seqNum = ", seqNum, " to the resulting set")
			blocks = append(blocks, blck)
		}
	}

	return blocks
}

// GetConfigHistoryRetriever returns the ConfigHistoryRetriever
func (lc *LedgerCommitter) GetConfigHistoryRetriever() (ledger.ConfigHistoryRetriever, error) {
	return lc.PeerLedgerSupport.GetConfigHistoryRetriever()
}

// GetMissingPvtDataTracker return the MissingPvtDataTracker
func (lc *LedgerCommitter) GetMissingPvtDataTracker() (ledger.MissingPvtDataTracker, error) {
	return lc.PeerLedgerSupport.GetMissingPvtDataTracker()
}

// CommitPvtDataOfOldBlocks commits the private data corresponding to already committed block
func (lc *LedgerCommitter) CommitPvtDataOfOldBlocks(reconciledPvtdata []*ledger.ReconciledPvtdata, unreconciled ledger.MissingPvtDataInfo) ([]*ledger.PvtdataHashMismatch, error) {
	return lc.PeerLedgerSupport.CommitPvtDataOfOldBlocks(reconciledPvtdata, unreconciled)
}

// Close closes the committer
func (lc *LedgerCommitter) Close() {
	lc.PeerLedgerSupport.Close()
}

// checkRWSetConflicts checks for read/write set conflicts between transactions
func (lc *LedgerCommitter) checkRWSetConflicts(tx1, tx2 *peer.Transaction) bool {
	// Extract RW sets from both transactions
	ns1Map, err := extractRWSet(tx1)
	if err != nil {
		logger.Errorf("Failed to extract RW set from tx1: %v", err)
		return false
	}

	ns2Map, err := extractRWSet(tx2)
	if err != nil {
		logger.Errorf("Failed to extract RW set from tx2: %v", err)
		return false
	}

	// Check for conflicts in each namespace
	for ns1, ns1Data := range ns1Map {
		ns2Data, exists := ns2Map[ns1]
		if !exists {
			continue // No conflict if namespaces don't overlap
		}

		// Unmarshal RW sets
		kvrw1 := &kvrwset.KVRWSet{}
		if err := proto.Unmarshal(ns1Data.Rwset, kvrw1); err != nil {
			logger.Errorf("Failed to unmarshal RW set for ns1: %v", err)
			return false
		}

		kvrw2 := &kvrwset.KVRWSet{}
		if err := proto.Unmarshal(ns2Data.Rwset, kvrw2); err != nil {
			logger.Errorf("Failed to unmarshal RW set for ns2: %v", err)
			return false
		}

		// Check for conflicts in public read/write sets
		if hasConflictInKVRWSet(kvrw1.Reads, kvrw2.Writes) ||
			hasConflictInKVRWSet(kvrw2.Reads, kvrw1.Writes) ||
			hasWriteWriteConflict(kvrw1.Writes, kvrw2.Writes) {
			return true
		}

		// Check for conflicts in private read/write sets
		for _, coll1 := range ns1Data.CollectionHashedRwset {
			hashedRW1 := &kvrwset.HashedRWSet{}
			if err := proto.Unmarshal(coll1.HashedRwset, hashedRW1); err != nil {
				logger.Errorf("Failed to unmarshal hashed RW set for collection %s: %v", coll1.CollectionName, err)
				continue
			}

			for _, coll2 := range ns2Data.CollectionHashedRwset {
				if coll1.CollectionName != coll2.CollectionName {
					continue
				}

				hashedRW2 := &kvrwset.HashedRWSet{}
				if err := proto.Unmarshal(coll2.HashedRwset, hashedRW2); err != nil {
					logger.Errorf("Failed to unmarshal hashed RW set for collection %s: %v", coll2.CollectionName, err)
					continue
				}

				if hasConflictInHashedRWSet(hashedRW1.HashedReads, hashedRW2.HashedWrites) ||
					hasConflictInHashedRWSet(hashedRW2.HashedReads, hashedRW1.HashedWrites) ||
					hasHashedWriteWriteConflict(hashedRW1.HashedWrites, hashedRW2.HashedWrites) {
					return true
				}
			}
		}
	}

	return false
}

// hasConflictInKVRWSet checks for conflicts between reads and writes in KVRWSet
func hasConflictInKVRWSet(reads []*kvrwset.KVRead, writes []*kvrwset.KVWrite) bool {
	// Create a map of write keys for efficient lookup
	writeKeys := make(map[string]struct{})
	for _, w := range writes {
		writeKeys[w.Key] = struct{}{}
	}

	// Check if any read key exists in writes
	for _, r := range reads {
		if _, exists := writeKeys[r.Key]; exists {
			return true
		}
	}
	return false
}

// hasConflictInHashedRWSet checks for conflicts between hashed reads and writes
func hasConflictInHashedRWSet(reads []*kvrwset.KVReadHash, writes []*kvrwset.KVWriteHash) bool {
	// Create a map of write key hashes for efficient lookup
	writeKeyHashes := make(map[string]struct{})
	for _, w := range writes {
		writeKeyHashes[string(w.KeyHash)] = struct{}{}
	}

	// Check if any read key hash exists in writes
	for _, r := range reads {
		if _, exists := writeKeyHashes[string(r.KeyHash)]; exists {
			return true
		}
	}
	return false
}

// hasWriteWriteConflict checks for conflicts between two write sets
func hasWriteWriteConflict(writes1 []*kvrwset.KVWrite, writes2 []*kvrwset.KVWrite) bool {
	// Create a map of write keys from the first set
	writeKeys := make(map[string]struct{})
	for _, w := range writes1 {
		writeKeys[w.Key] = struct{}{}
	}

	// Check if any key from second set exists in first set
	for _, w := range writes2 {
		if _, exists := writeKeys[w.Key]; exists {
			return true
		}
	}
	return false
}

// hasHashedWriteWriteConflict checks for conflicts between two hashed write sets
func hasHashedWriteWriteConflict(writes1 []*kvrwset.KVWriteHash, writes2 []*kvrwset.KVWriteHash) bool {
	// Create a map of write key hashes from the first set
	writeKeyHashes := make(map[string]struct{})
	for _, w := range writes1 {
		writeKeyHashes[string(w.KeyHash)] = struct{}{}
	}

	// Check if any key hash from second set exists in first set
	for _, w := range writes2 {
		if _, exists := writeKeyHashes[string(w.KeyHash)]; exists {
			return true
		}
	}
	return false
}

// extractRWSet extracts read/write sets from a transaction
func extractRWSet(tx *peer.Transaction) (map[string]*rwset.NsReadWriteSet, error) {
	rwSets := make(map[string]*rwset.NsReadWriteSet)

	for _, action := range tx.Actions {
		chaincodeAction := &peer.ChaincodeAction{}
		if err := proto.Unmarshal(action.Payload, chaincodeAction); err != nil {
			return nil, err
		}

		// Extract read/write set from chaincode action
		if chaincodeAction.Results == nil {
			continue
		}

		txRWSet := &rwset.TxReadWriteSet{}
		if err := proto.Unmarshal(chaincodeAction.Results, txRWSet); err != nil {
			return nil, err
		}

		// Process each namespace's read/write set
		for _, nsRWSet := range txRWSet.NsRwset {
			ns := nsRWSet.Namespace
			if _, exists := rwSets[ns]; !exists {
				rwSets[ns] = &rwset.NsReadWriteSet{
					Namespace:             ns,
					Rwset:                 nsRWSet.Rwset,
					CollectionHashedRwset: nsRWSet.CollectionHashedRwset,
				}
			}
		}
	}

	return rwSets, nil
}
