/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

/*
Transaction Dependency Tracking Enhancement for Hyperledger Fabric Endorser

This implementation adds transaction dependency tracking to the Fabric endorser component.
The primary features include:

1. Variable Tracking: Maintains a hashmap of variables (keys) that transactions operate on,
   along with their current values and the transaction that last modified them.

2. Dependency Detection: When a transaction accesses a variable that exists in the hashmap,
   the system marks it as dependent on the transaction that previously modified that variable.

3. Dependency Information in Responses: The endorser includes dependency information in the
   endorsement response, allowing clients to be aware of transaction dependencies.

4. Expiry Mechanism: Each tracked variable has an expiry time. A background routine cleans up
   expired entries to prevent the hashmap from growing indefinitely.

5. Metrics Collection: The implementation includes metrics to track dependency-related statistics.

Transaction Flow with Dependencies:
1. Client sends transaction proposal to endorser
2. Endorser simulates the transaction to determine which variables it operates on
3. For each variable:
   a. If the variable is not in the hashmap, it's added with no dependencies
   b. If the variable is in the hashmap, the transaction is marked as dependent on the previous tx
4. The endorser includes dependency information in the response
5. Client can use this information to manage transaction ordering during submission

This implementation helps address potential transaction conflicts and ordering issues in
Fabric applications where transactions might operate on shared state.
*/

package endorser

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric-protos-go/transientstore"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var endorserLogger = flogging.MustGetLogger("endorser")

// TransactionDependencyInfo represents information about a transaction dependency
type TransactionDependencyInfo struct {
	Value         []byte    // The current value of the variable
	DependentTxID string    // ID of the transaction this depends on (if any)
	ExpiryTime    time.Time // When this endorsement expires
	HasDependency bool      // Whether this transaction has a dependency
}

// The Jira issue that documents Endorser flow along with its relationship to
// the lifecycle chaincode - https://jira.hyperledger.org/browse/FAB-181

//go:generate counterfeiter -o fake/prvt_data_distributor.go --fake-name PrivateDataDistributor . PrivateDataDistributor

type PrivateDataDistributor interface {
	DistributePrivateData(channel string, txID string, privateData *transientstore.TxPvtReadWriteSetWithConfigInfo, blkHt uint64) error
}

// Support contains functions that the endorser requires to execute its tasks
type Support interface {
	identity.SignerSerializer
	// GetTxSimulator returns the transaction simulator for the specified ledger
	// a client may obtain more than one such simulator; they are made unique
	// by way of the supplied txid
	GetTxSimulator(ledgername string, txid string) (ledger.TxSimulator, error)

	// GetHistoryQueryExecutor gives handle to a history query executor for the
	// specified ledger
	GetHistoryQueryExecutor(ledgername string) (ledger.HistoryQueryExecutor, error)

	// GetTransactionByID retrieves a transaction by id
	GetTransactionByID(chid, txID string) (*pb.ProcessedTransaction, error)

	// IsSysCC returns true if the name matches a system chaincode's
	// system chaincode names are system, chain wide
	IsSysCC(name string) bool

	// Execute - execute proposal, return original response of chaincode
	Execute(txParams *ccprovider.TransactionParams, name string, input *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error)

	// ExecuteLegacyInit - executes a deployment proposal, return original response of chaincode
	ExecuteLegacyInit(txParams *ccprovider.TransactionParams, name, version string, spec *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error)

	// ChaincodeEndorsementInfo returns the information from lifecycle required to endorse the chaincode.
	ChaincodeEndorsementInfo(channelID, chaincodeID string, txsim ledger.QueryExecutor) (*lifecycle.ChaincodeEndorsementInfo, error)

	// CheckACL checks the ACL for the resource for the channel using the
	// SignedProposal from which an id can be extracted for testing against a policy
	CheckACL(channelID string, signedProp *pb.SignedProposal) error

	// EndorseWithPlugin endorses the response with a plugin
	EndorseWithPlugin(pluginName, channnelID string, prpBytes []byte, signedProposal *pb.SignedProposal) (*pb.Endorsement, []byte, error)

	// GetLedgerHeight returns ledger height for given channelID
	GetLedgerHeight(channelID string) (uint64, error)

	// GetDeployedCCInfoProvider returns ledger.DeployedChaincodeInfoProvider
	GetDeployedCCInfoProvider() ledger.DeployedChaincodeInfoProvider
}

//go:generate counterfeiter -o fake/channel_fetcher.go --fake-name ChannelFetcher . ChannelFetcher

// ChannelFetcher fetches the channel context for a given channel ID.
type ChannelFetcher interface {
	Channel(channelID string) *Channel
}

type Channel struct {
	IdentityDeserializer msp.IdentityDeserializer
}

// Endorser provides the Endorser service ProcessProposal
type Endorser struct {
	ChannelFetcher         ChannelFetcher
	LocalMSP               msp.IdentityDeserializer
	PrivateDataDistributor PrivateDataDistributor
	Support                Support
	PvtRWSetAssembler      PvtRWSetAssembler
	Metrics                *Metrics
	// New fields for transaction dependency tracking
	VariableMap     map[string]TransactionDependencyInfo
	VariableMapLock sync.RWMutex
	// Configuration for endorsement expiry time
	EndorsementExpiryDuration time.Duration
}

// NewEndorser creates a new instance of Endorser with the given dependencies
func NewEndorser(channelFetcher ChannelFetcher, localMSP msp.IdentityDeserializer,
	pvtDataDistributor PrivateDataDistributor, support Support,
	pvtRWSetAssembler PvtRWSetAssembler, metrics *Metrics) *Endorser {
	endorser := &Endorser{
		ChannelFetcher:            channelFetcher,
		LocalMSP:                  localMSP,
		PrivateDataDistributor:    pvtDataDistributor,
		Support:                   support,
		PvtRWSetAssembler:         pvtRWSetAssembler,
		Metrics:                   metrics,
		VariableMap:               make(map[string]TransactionDependencyInfo),
		EndorsementExpiryDuration: 5 * time.Minute, // Default 5 minutes endorsement expiry
	}

	// Start a background goroutine to clean up expired entries
	go endorser.cleanupExpiredDependencies()

	return endorser
}

// cleanupExpiredDependencies periodically checks for and removes expired entries from the variable map
func (e *Endorser) cleanupExpiredDependencies() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()

			e.VariableMapLock.Lock()
			// Check each entry for expiration
			removedCount := 0
			for key, info := range e.VariableMap {
				if now.After(info.ExpiryTime) {
					delete(e.VariableMap, key)
					removedCount++
					endorserLogger.Debugf("Removed expired dependency for variable %s", key)
				}
			}

			// Update metrics
			if removedCount > 0 && e.Metrics.ExpiredDependenciesRemoved != nil {
				e.Metrics.ExpiredDependenciesRemoved.Add(float64(removedCount))
			}

			if e.Metrics.DependencyMapSize != nil {
				e.Metrics.DependencyMapSize.Set(float64(len(e.VariableMap)))
			}

			endorserLogger.Infof("Dependency cleanup completed: %d expired entries removed, current map size: %d",
				removedCount, len(e.VariableMap))

			e.VariableMapLock.Unlock()
		}
	}
}

// call specified chaincode (system or user)
func (e *Endorser) callChaincode(txParams *ccprovider.TransactionParams, input *pb.ChaincodeInput, chaincodeName string) (*pb.Response, *pb.ChaincodeEvent, error) {
	defer func(start time.Time) {
		logger := endorserLogger.WithOptions(zap.AddCallerSkip(1))
		logger = decorateLogger(logger, txParams)
		elapsedMillisec := time.Since(start).Milliseconds()
		logger.Infof("finished chaincode: %s duration: %dms", chaincodeName, elapsedMillisec)
	}(time.Now())

	meterLabels := []string{
		"channel", txParams.ChannelID,
		"chaincode", chaincodeName,
	}

	res, ccevent, err := e.Support.Execute(txParams, chaincodeName, input)
	if err != nil {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, err
	}

	// per doc anything < 400 can be sent as TX.
	// fabric errors will always be >= 400 (ie, unambiguous errors )
	// "lscc" will respond with status 200 or 500 (ie, unambiguous OK or ERROR)
	if res.Status >= shim.ERRORTHRESHOLD {
		return res, nil, nil
	}

	// Unless this is the weirdo LSCC case, just return
	if chaincodeName != "lscc" || len(input.Args) < 3 || (string(input.Args[0]) != "deploy" && string(input.Args[0]) != "upgrade") {
		return res, ccevent, nil
	}

	// ----- BEGIN -  SECTION THAT MAY NEED TO BE DONE IN LSCC ------
	// if this a call to deploy a chaincode, We need a mechanism
	// to pass TxSimulator into LSCC. Till that is worked out this
	// special code does the actual deploy, upgrade here so as to collect
	// all state under one TxSimulator
	//
	// NOTE that if there's an error all simulation, including the chaincode
	// table changes in lscc will be thrown away
	cds, err := protoutil.UnmarshalChaincodeDeploymentSpec(input.Args[2])
	if err != nil {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, err
	}

	// this should not be a system chaincode
	if e.Support.IsSysCC(cds.ChaincodeSpec.ChaincodeId.Name) {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, errors.Errorf("attempting to deploy a system chaincode %s/%s", cds.ChaincodeSpec.ChaincodeId.Name, txParams.ChannelID)
	}

	if len(cds.CodePackage) != 0 {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, errors.Errorf("lscc upgrade/deploy should not include a code packages")
	}

	_, _, err = e.Support.ExecuteLegacyInit(txParams, cds.ChaincodeSpec.ChaincodeId.Name, cds.ChaincodeSpec.ChaincodeId.Version, cds.ChaincodeSpec.Input)
	if err != nil {
		// increment the failure to indicate instantion/upgrade failures
		meterLabels = []string{
			"channel", txParams.ChannelID,
			"chaincode", cds.ChaincodeSpec.ChaincodeId.Name,
		}
		e.Metrics.InitFailed.With(meterLabels...).Add(1)
		return nil, nil, err
	}

	return res, ccevent, err
}

// extractTransactionDependencies identifies variables that the transaction operates on
// from the simulation results. This is a simplified implementation - you'll need to adapt
// this to extract the actual variables based on your application's specific data model.
func (e *Endorser) extractTransactionDependencies(simResult *ledger.TxSimulationResults) (map[string][]byte, error) {
	// This is a simplified implementation - in a real scenario, you would:
	// 1. Analyze the read-write sets to identify the state variables
	// 2. Extract the variable identifiers based on your application model
	// 3. Determine the current values of those variables

	dependencies := make(map[string][]byte)

	// Extract variables from public state
	if simResult.PubSimulationResults != nil {
		for _, nsRWSet := range simResult.PubSimulationResults.NsRwset {
			namespace := nsRWSet.Namespace

			// Skip system chaincodes
			if e.Support.IsSysCC(namespace) {
				continue
			}

			// Extract write keys - these are the variables being modified
			kvRWSet := &kvrwset.KVRWSet{}
			if err := proto.Unmarshal(nsRWSet.Rwset, kvRWSet); err != nil {
				endorserLogger.Warningf("Failed to unmarshal rwset for namespace %s: %s", namespace, err)
				continue
			}
			for _, write := range kvRWSet.Writes {
				key := namespace + ":" + string(write.Key)
				dependencies[key] = write.Value
				endorserLogger.Debugf("Transaction dependency identified: %s", key)
			}

			// You might also want to track read keys, as they indicate dependencies
			// In a full implementation, you would need to decide which of these reads
			// are critical for determining transaction dependencies
			if kvRWSet == nil {
				kvRWSet = &kvrwset.KVRWSet{}
				if err := proto.Unmarshal(nsRWSet.Rwset, kvRWSet); err != nil {
					endorserLogger.Warningf("Failed to unmarshal rwset for namespace %s: %s", namespace, err)
					continue
				}
			}
			for _, read := range kvRWSet.Reads {
				key := namespace + ":" + string(read.Key)
				// Only add if not already added as a write
				if _, exists := dependencies[key]; !exists {
					if read.Version != nil {
						// Convert version to a byte array representation
						versionBytes := []byte(fmt.Sprintf("%d-%d", read.Version.BlockNum, read.Version.TxNum))
						dependencies[key] = versionBytes
					} else {
						dependencies[key] = []byte{}
					}
					endorserLogger.Debugf("Transaction read dependency identified: %s", key)
				}
			}
		}
	}

	// Process private data if needed
	// Note: For private data, you need to handle collection-specific dependencies
	if simResult.PvtSimulationResults != nil {
		for _, pvtRWSet := range simResult.PvtSimulationResults.NsPvtRwset {
			namespace := pvtRWSet.Namespace

			// Skip system chaincodes
			if e.Support.IsSysCC(namespace) {
				continue
			}

			for _, collection := range pvtRWSet.CollectionPvtRwset {
				collectionName := collection.CollectionName

				collKVRWSet := &kvrwset.KVRWSet{}
				if err := proto.Unmarshal(collection.Rwset, collKVRWSet); err != nil {
					endorserLogger.Warningf("Failed to unmarshal collection rwset for namespace %s, collection %s: %s",
						namespace, collectionName, err)
					continue
				}

				for _, write := range collKVRWSet.Writes {
					key := namespace + ":" + collectionName + ":" + string(write.Key)
					dependencies[key] = write.Value
					endorserLogger.Debugf("Private data dependency identified: %s", key)
				}

				// Similarly, you might track read dependencies for private data
				for _, read := range collKVRWSet.Reads {
					key := namespace + ":" + collectionName + ":" + string(read.Key)
					if _, exists := dependencies[key]; !exists {
						if read.Version != nil {
							// Convert version to a byte array representation
							versionBytes := []byte(fmt.Sprintf("%d-%d", read.Version.BlockNum, read.Version.TxNum))
							dependencies[key] = versionBytes
						} else {
							dependencies[key] = []byte{}
						}
						endorserLogger.Debugf("Private data read dependency identified: %s", key)
					}
				}
			}
		}
	}

	return dependencies, nil
}

// SimulateProposal simulates the proposal by calling the chaincode
func (e *Endorser) simulateProposal(txParams *ccprovider.TransactionParams, chaincodeName string, chaincodeInput *pb.ChaincodeInput) (*pb.Response, []byte, *pb.ChaincodeEvent, *pb.ChaincodeInterest, error) {
	logger := decorateLogger(endorserLogger, txParams)

	meterLabels := []string{
		"channel", txParams.ChannelID,
		"chaincode", chaincodeName,
	}

	// ---3. execute the proposal and get simulation results
	res, ccevent, err := e.callChaincode(txParams, chaincodeInput, chaincodeName)
	if err != nil {
		logger.Errorf("failed to invoke chaincode %s, error: %+v", chaincodeName, err)
		return nil, nil, nil, nil, err
	}

	if txParams.TXSimulator == nil {
		return res, nil, ccevent, nil, nil
	}

	// Note, this is a little goofy, as if there is private data, Done() gets called
	// early, so this is invoked multiple times, but that is how the code worked before
	// this change, so, should be safe.  Long term, let's move the Done up to the create.
	defer txParams.TXSimulator.Done()

	simResult, err := txParams.TXSimulator.GetTxSimulationResults()
	if err != nil {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, nil, nil, err
	}

	if simResult.PvtSimulationResults != nil {
		if chaincodeName == "lscc" {
			// TODO: remove once we can store collection configuration outside of LSCC
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, errors.New("Private data is forbidden to be used in instantiate")
		}
		pvtDataWithConfig, err := AssemblePvtRWSet(txParams.ChannelID, simResult.PvtSimulationResults, txParams.TXSimulator, e.Support.GetDeployedCCInfoProvider())
		// To read collection config need to read collection updates before
		// releasing the lock, hence txParams.TXSimulator.Done()  moved down here
		txParams.TXSimulator.Done()

		if err != nil {
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, errors.WithMessage(err, "failed to obtain collections config")
		}
		endorsedAt, err := e.Support.GetLedgerHeight(txParams.ChannelID)
		if err != nil {
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, errors.WithMessage(err, fmt.Sprintf("failed to obtain ledger height for channel '%s'", txParams.ChannelID))
		}
		// Add ledger height at which transaction was endorsed,
		// `endorsedAt` is obtained from the block storage and at times this could be 'endorsement Height + 1'.
		// However, since we use this height only to select the configuration (3rd parameter in distributePrivateData) and
		// manage transient store purge for orphaned private writesets (4th parameter in distributePrivateData), this works for now.
		// Ideally, ledger should add support in the simulator as a first class function `GetHeight()`.
		pvtDataWithConfig.EndorsedAt = endorsedAt
		if err := e.PrivateDataDistributor.DistributePrivateData(txParams.ChannelID, txParams.TxID, pvtDataWithConfig, endorsedAt); err != nil {
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, err
		}
	}

	ccInterest, err := e.buildChaincodeInterest(simResult)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	pubSimResBytes, err := simResult.GetPubSimulationBytes()
	if err != nil {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, nil, nil, err
	}

	return res, pubSimResBytes, ccevent, ccInterest, nil
}

// preProcess checks the tx proposal headers, uniqueness and ACL
func (e *Endorser) preProcess(up *UnpackedProposal, channel *Channel) error {
	// at first, we check whether the message is valid

	err := up.Validate(channel.IdentityDeserializer)
	if err != nil {
		e.Metrics.ProposalValidationFailed.Add(1)
		return errors.WithMessage(err, "error validating proposal")
	}

	if up.ChannelHeader.ChannelId == "" {
		// chainless proposals do not/cannot affect ledger and cannot be submitted as transactions
		// ignore uniqueness checks; also, chainless proposals are not validated using the policies
		// of the chain since by definition there is no chain; they are validated against the local
		// MSP of the peer instead by the call to ValidateUnpackProposal above
		return nil
	}

	// labels that provide context for failure metrics
	meterLabels := []string{
		"channel", up.ChannelHeader.ChannelId,
		"chaincode", up.ChaincodeName,
	}

	// Here we handle uniqueness check and ACLs for proposals targeting a chain
	// Notice that ValidateProposalMessage has already verified that TxID is computed properly
	if _, err = e.Support.GetTransactionByID(up.ChannelHeader.ChannelId, up.ChannelHeader.TxId); err == nil {
		// increment failure due to duplicate transactions. Useful for catching replay attacks in
		// addition to benign retries
		e.Metrics.DuplicateTxsFailure.With(meterLabels...).Add(1)
		return errors.Errorf("duplicate transaction found [%s]. Creator [%x]", up.ChannelHeader.TxId, up.SignatureHeader.Creator)
	}

	// check ACL only for application chaincodes; ACLs
	// for system chaincodes are checked elsewhere
	if !e.Support.IsSysCC(up.ChaincodeName) {
		// check that the proposal complies with the Channel's writers
		if err = e.Support.CheckACL(up.ChannelHeader.ChannelId, up.SignedProposal); err != nil {
			e.Metrics.ProposalACLCheckFailed.With(meterLabels...).Add(1)
			return err
		}
	}

	return nil
}

// ProcessProposal process the Proposal
// Errors related to the proposal itself are returned with an error that results in a grpc error.
// Errors related to proposal processing (either infrastructure errors or chaincode errors) are returned with a nil error,
// clients are expected to look at the ProposalResponse response status code (e.g. 500) and message.
func (e *Endorser) ProcessProposal(ctx context.Context, signedProp *pb.SignedProposal) (*pb.ProposalResponse, error) {
	// start time for computing elapsed time metric for successfully endorsed proposals
	startTime := time.Now()
	e.Metrics.ProposalsReceived.Add(1)

	addr := util.ExtractRemoteAddress(ctx)
	endorserLogger.Debug("request from", addr)

	// variables to capture proposal duration metric
	success := false

	up, err := UnpackProposal(signedProp)
	if err != nil {
		e.Metrics.ProposalValidationFailed.Add(1)
		endorserLogger.Warnw("Failed to unpack proposal", "error", err.Error())
		return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: err.Error()}}, err
	}

	var channel *Channel
	if up.ChannelID() != "" {
		channel = e.ChannelFetcher.Channel(up.ChannelID())
		if channel == nil {
			return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: fmt.Sprintf("channel '%s' not found", up.ChannelHeader.ChannelId)}}, nil
		}
	} else {
		channel = &Channel{
			IdentityDeserializer: e.LocalMSP,
		}
	}

	// 0 -- check and validate
	err = e.preProcess(up, channel)
	if err != nil {
		endorserLogger.Warnw("Failed to preProcess proposal", "error", err.Error())
		return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: err.Error()}}, err
	}

	defer func() {
		meterLabels := []string{
			"channel", up.ChannelHeader.ChannelId,
			"chaincode", up.ChaincodeName,
			"success", strconv.FormatBool(success),
		}
		e.Metrics.ProposalDuration.With(meterLabels...).Observe(time.Since(startTime).Seconds())
	}()

	pResp, err := e.ProcessProposalSuccessfullyOrError(up)
	if err != nil {
		endorserLogger.Warnw("Failed to invoke chaincode", "channel", up.ChannelHeader.ChannelId, "chaincode", up.ChaincodeName, "error", err.Error())
		// Return a nil error since clients are expected to look at the ProposalResponse response status code (500) and message.
		return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: err.Error()}}, nil
	}

	if pResp.Endorsement != nil || up.ChannelHeader.ChannelId == "" {
		// We mark the tx as successful only if it was successfully endorsed, or
		// if it was a system chaincode on a channel-less channel and therefore
		// cannot be endorsed.
		success = true

		// total failed proposals = ProposalsReceived-SuccessfulProposals
		e.Metrics.SuccessfulProposals.Add(1)
	}
	return pResp, nil
}

func (e *Endorser) ProcessProposalSuccessfullyOrError(up *UnpackedProposal) (*pb.ProposalResponse, error) {
	txParams := &ccprovider.TransactionParams{
		ChannelID:  up.ChannelHeader.ChannelId,
		TxID:       up.ChannelHeader.TxId,
		SignedProp: up.SignedProposal,
		Proposal:   up.Proposal,
	}

	logger := decorateLogger(endorserLogger, txParams)

	if acquireTxSimulator(up.ChannelHeader.ChannelId, up.ChaincodeName) {
		txSim, err := e.Support.GetTxSimulator(up.ChannelID(), up.TxID())
		if err != nil {
			return nil, err
		}

		// txsim acquires a shared lock on the stateDB. As this would impact the block commits (i.e., commit
		// of valid write-sets to the stateDB), we must release the lock as early as possible.
		// Hence, this txsim object is closed in simulateProposal() as soon as the tx is simulated and
		// rwset is collected before gossip dissemination if required for privateData. For safety, we
		// add the following defer statement and is useful when an error occur. Note that calling
		// txsim.Done() more than once does not cause any issue. If the txsim is already
		// released, the following txsim.Done() simply returns.
		defer txSim.Done()

		hqe, err := e.Support.GetHistoryQueryExecutor(up.ChannelID())
		if err != nil {
			return nil, err
		}

		txParams.TXSimulator = txSim
		txParams.HistoryQueryExecutor = hqe
	}

	cdLedger, err := e.Support.ChaincodeEndorsementInfo(up.ChannelID(), up.ChaincodeName, txParams.TXSimulator)
	if err != nil {
		return nil, errors.WithMessagef(err, "make sure the chaincode %s has been successfully defined on channel %s and try again", up.ChaincodeName, up.ChannelID())
	}

	// 1 -- simulate
	res, simulationResult, ccevent, ccInterest, err := e.simulateProposal(txParams, up.ChaincodeName, up.Input)
	if err != nil {
		return nil, errors.WithMessage(err, "error in simulation")
	}

	// Handle the simulation results for transaction dependency tracking
	// Only proceed with endorsement if simulation is successful
	if res.Status >= shim.ERROR {
		// If simulation failed, return the response without endorsement
		return &pb.ProposalResponse{
			Response: res,
		}, nil
	}

	// Extract transaction dependencies
	simResults, err := txParams.TXSimulator.GetTxSimulationResults()
	if err != nil {
		return nil, errors.WithMessage(err, "error getting simulation results")
	}

	dependencies, err := e.extractTransactionDependencies(simResults)
	if err != nil {
		return nil, errors.WithMessage(err, "error extracting transaction dependencies")
	}

	// If no dependencies were found, we can skip the dependency tracking
	if len(dependencies) == 0 {
		logger.Debug("No dependencies found for transaction")
	} else {
		logger.Debugf("Found %d dependencies for transaction", len(dependencies))
	}

	// Check for dependencies and update the hashmap
	hasDependency := false
	var dependentTxID string

	// Lock for concurrent access to the dependency map
	e.VariableMapLock.Lock()
	defer e.VariableMapLock.Unlock()

	// Set the dependency map size metric
	if e.Metrics.DependencyMapSize != nil {
		e.Metrics.DependencyMapSize.Set(float64(len(e.VariableMap)))
	}

	// Check each variable this transaction operates on
	for varKey, varValue := range dependencies {
		if dependencyInfo, exists := e.VariableMap[varKey]; exists {
			// Variable exists in the map - this transaction has a dependency
			hasDependency = true
			dependentTxID = dependencyInfo.DependentTxID

			logger.Debugf("Transaction dependency found: %s depends on transaction %s",
				up.ChannelHeader.TxId, dependencyInfo.DependentTxID)

			// Update the variable map with new value
			e.VariableMap[varKey] = TransactionDependencyInfo{
				Value:         varValue,
				DependentTxID: up.ChannelHeader.TxId,
				ExpiryTime:    time.Now().Add(e.EndorsementExpiryDuration),
				HasDependency: true,
			}
		} else {
			// New variable, add to the map
			logger.Debugf("New variable tracked: %s for transaction %s",
				varKey, up.ChannelHeader.TxId)

			e.VariableMap[varKey] = TransactionDependencyInfo{
				Value:         varValue,
				DependentTxID: up.ChannelHeader.TxId,
				ExpiryTime:    time.Now().Add(e.EndorsementExpiryDuration),
				HasDependency: false,
			}
		}
	}

	// Update metrics
	if hasDependency && e.Metrics.TransactionsWithDependencies != nil {
		e.Metrics.TransactionsWithDependencies.Add(1)
	}

	cceventBytes, err := CreateCCEventBytes(ccevent)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal chaincode event")
	}

	prpBytes, err := protoutil.GetBytesProposalResponsePayload(up.ProposalHash, res, simulationResult, cceventBytes, &pb.ChaincodeID{
		Name:    up.ChaincodeName,
		Version: cdLedger.Version,
	})
	if err != nil {
		logger.Warning("Failed marshaling the proposal response payload to bytes", err)
		return nil, errors.WithMessage(err, "failed to create the proposal response")
	}

	// At this point, the transaction has been successfully simulated and we have extracted
	// the dependencies. Now we need to endorse the transaction and include the dependency info.

	// if error, capture endorsement failure metric
	meterLabels := []string{
		"channel", up.ChannelID(),
		"chaincode", up.ChaincodeName,
	}

	switch {
	case res.Status >= shim.ERROR:
		return &pb.ProposalResponse{
			Response: res,
			Payload:  prpBytes,
			Interest: ccInterest,
		}, nil
	case up.ChannelID() == "":
		// Chaincode invocations without a channel ID is a broken concept
		// that should be removed in the future.  For now, return unendorsed
		// success.
		return &pb.ProposalResponse{
			Response: res,
		}, nil
	case res.Status >= shim.ERRORTHRESHOLD:
		meterLabels = append(meterLabels, "chaincodeerror", strconv.FormatBool(true))
		e.Metrics.EndorsementsFailed.With(meterLabels...).Add(1)
		logger.Debugf("chaincode error %d", res.Status)
		return &pb.ProposalResponse{
			Response: res,
		}, nil
	}

	escc := cdLedger.EndorsementPlugin

	logger.Debugf("escc for chaincode %s is %s", up.ChaincodeName, escc)

	// Note, mPrpBytes is the same as prpBytes by default endorsement plugin, but others could change it.
	endorsement, mPrpBytes, err := e.Support.EndorseWithPlugin(escc, up.ChannelID(), prpBytes, up.SignedProposal)
	if err != nil {
		meterLabels = append(meterLabels, "chaincodeerror", strconv.FormatBool(false))
		e.Metrics.EndorsementsFailed.With(meterLabels...).Add(1)
		return nil, errors.WithMessage(err, "endorsing with plugin failed")
	}

	// In a real implementation, you might want to use a more structured approach,
	// such as adding these fields to the TransactionEndorsement message or
	// using the Metadata field in the ProposalResponse

	res.Message = fmt.Sprintf("%s; DependencyInfo:HasDependency=%v,DependentTxID=%s,ExpiryTime=%d",
		res.Message, hasDependency, dependentTxID, time.Now().Add(e.EndorsementExpiryDuration).Unix())

	return &pb.ProposalResponse{
		Version:     1,
		Endorsement: endorsement,
		Payload:     mPrpBytes,
		Response:    res,
		Interest:    ccInterest,
		// Include the dependency information as a custom field
		// In a production system, you would likely modify the Fabric protos to include these fields directly
	}, nil
}

// Using the simulation results, build the ChaincodeInterest structure that the client can pass to the discovery service
// to get the correct endorsement policy for the chaincode(s) and any collections encountered.
func (e *Endorser) buildChaincodeInterest(simResult *ledger.TxSimulationResults) (*pb.ChaincodeInterest, error) {
	// build a structure that collates all the information needed for the chaincode interest:
	policies, err := parseWritesetMetadata(simResult.WritesetMetadata)
	if err != nil {
		return nil, err
	}

	// There might be public states that are read and not written.  Need to add these to the policyRequired structure.
	// This will also include private reads, because the hashed read will appear in the public RWset.
	for _, nsrws := range simResult.PubSimulationResults.GetNsRwset() {
		if e.Support.IsSysCC(nsrws.Namespace) {
			// skip system chaincodes
			continue
		}
		if _, ok := policies.policyRequired[nsrws.Namespace]; !ok {
			// There's a public RWset for this namespace, but no public or private writes, so chaincode policy is required.
			policies.add(nsrws.Namespace, "", true)
		}
	}

	for chaincode, collections := range simResult.PrivateReads {
		for collection := range collections {
			policies.add(chaincode, collection, true)
		}
	}

	ccInterest := &pb.ChaincodeInterest{}
	for chaincode, collections := range policies.policyRequired {
		if e.Support.IsSysCC(chaincode) {
			// skip system chaincodes
			continue
		}
		for collection := range collections {
			ccCall := &pb.ChaincodeCall{
				Name: chaincode,
			}
			if collection == "" { // the empty collection name here represents the public RWset
				keyPolicies := policies.sbePolicies[chaincode]
				if len(keyPolicies) > 0 {
					// For simplicity, we'll always add the SBE policies to the public ChaincodeCall, and set the disregard flag if the chaincode policy is not required.
					ccCall.KeyPolicies = keyPolicies
					if !policies.requireChaincodePolicy(chaincode) {
						ccCall.DisregardNamespacePolicy = true
					}
				} else if !policies.requireChaincodePolicy(chaincode) {
					continue
				}
			} else {
				// Since each collection in a chaincode could have different values of the NoPrivateReads flag, create a new Chaincode entry for each.
				ccCall.CollectionNames = []string{collection}
				ccCall.NoPrivateReads = !simResult.PrivateReads.Exists(chaincode, collection)
			}
			ccInterest.Chaincodes = append(ccInterest.Chaincodes, ccCall)
		}
	}

	endorserLogger.Debug("ccInterest", ccInterest)
	return ccInterest, nil
}

type metadataPolicies struct {
	// Map of SBE policies: namespace -> array of policies.
	sbePolicies map[string][]*common.SignaturePolicyEnvelope
	// Whether the chaincode/collection policy is required for endorsement: namespace -> collection -> isRequired
	// Empty collection name represents the public rwset
	// Each entry in this map represents a ChaincodeCall structure in the final ChaincodeInterest.  The boolean
	// flag isRequired is used to control whether the DisregardNamespacePolicy flag should be set.
	policyRequired map[string]map[string]bool
}

func parseWritesetMetadata(metadata ledger.WritesetMetadata) (*metadataPolicies, error) {
	mp := &metadataPolicies{
		sbePolicies:    map[string][]*common.SignaturePolicyEnvelope{},
		policyRequired: map[string]map[string]bool{},
	}
	for ns, cmap := range metadata {
		mp.policyRequired[ns] = map[string]bool{"": false}
		for coll, kmap := range cmap {
			// look through each of the states that were written to
			for _, stateMetadata := range kmap {
				if policyBytes, sbeExists := stateMetadata[pb.MetaDataKeys_VALIDATION_PARAMETER.String()]; sbeExists {
					policy, err := protoutil.UnmarshalSignaturePolicy(policyBytes)
					if err != nil {
						return nil, err
					}
					mp.sbePolicies[ns] = append(mp.sbePolicies[ns], policy)
				} else {
					// the state metadata doesn't contain data relating to SBE policy, so the chaincode/collection policy is required
					mp.policyRequired[ns][coll] = true
				}
			}
		}
	}

	return mp, nil
}

func (mp *metadataPolicies) add(ns string, coll string, required bool) {
	if entry, ok := mp.policyRequired[ns]; ok {
		entry[coll] = required
	} else {
		mp.policyRequired[ns] = map[string]bool{coll: required}
	}
}

func (mp *metadataPolicies) requireChaincodePolicy(ns string) bool {
	// if any of the states (keys) were written to without those states having a SBE policy, then the chaincode policy will be required for this namespace
	return mp.policyRequired[ns][""]
}

// determine whether or not a transaction simulator should be
// obtained for a proposal.
func acquireTxSimulator(chainID string, chaincodeName string) bool {
	if chainID == "" {
		return false
	}

	// ¯\_(ツ)_/¯ locking.
	// Don't get a simulator for the query and config system chaincode.
	// These don't need the simulator and its read lock results in deadlocks.
	switch chaincodeName {
	case "qscc", "cscc":
		return false
	default:
		return true
	}
}

// shorttxid replicates the chaincode package function to shorten txids.
// ~~TODO utilize a common shorttxid utility across packages.~~
// TODO use a formal type for transaction ID and make it a stringer
func shorttxid(txid string) string {
	if len(txid) < 8 {
		return txid
	}
	return txid[0:8]
}

func CreateCCEventBytes(ccevent *pb.ChaincodeEvent) ([]byte, error) {
	if ccevent == nil {
		return nil, nil
	}

	return proto.Marshal(ccevent)
}

func decorateLogger(logger *flogging.FabricLogger, txParams *ccprovider.TransactionParams) *flogging.FabricLogger {
	return logger.With("channel", txParams.ChannelID, "txID", shorttxid(txParams.TxID))
}
