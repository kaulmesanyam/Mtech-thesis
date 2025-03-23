/*
Copyright State Street Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"github.com/hyperledger/fabric/common/metrics"
)

var (
	proposalDurationHistogramOpts = metrics.HistogramOpts{
		Namespace:    "endorser",
		Name:         "proposal_duration",
		Help:         "The time to complete a proposal.",
		LabelNames:   []string{"channel", "chaincode", "success"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}.%{success}",
	}

	receivedProposalsCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "proposals_received",
		Help:      "The number of proposals received.",
	}

	successfulProposalsCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "successful_proposals",
		Help:      "The number of successful proposals.",
	}

	proposalValidationFailureCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "proposal_validation_failures",
		Help:      "The number of proposals that have failed initial validation.",
	}

	proposalChannelACLFailureOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "proposal_acl_failures",
		Help:         "The number of proposals that failed ACL checks.",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	initFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "chaincode_instantiation_failures",
		Help:         "The number of chaincode instantiations or upgrade that have failed.",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	endorsementFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "endorsement_failures",
		Help:         "The number of failed endorsements.",
		LabelNames:   []string{"channel", "chaincode", "chaincodeerror"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}.%{chaincodeerror}",
	}

	duplicateTxsFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "duplicate_transaction_failures",
		Help:         "The number of failed proposals due to duplicate transaction ID.",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	simulationFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "proposal_simulation_failures",
		Help:         "The number of failed proposal simulations",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	// New metrics for transaction dependency tracking
	transactionsWithDependenciesCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "transactions_with_dependencies",
		Help:         "The number of transactions with dependencies on other transactions.",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	dependencyMapSizeGaugeOpts = metrics.GaugeOpts{
		Namespace: "endorser",
		Name:      "dependency_map_size",
		Help:      "The current size of the transaction dependency map.",
	}

	expiredDependenciesRemovedCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "expired_dependencies_removed",
		Help:      "The number of expired transaction dependencies removed during cleanup.",
	}

	// Circuit breaker metrics
	leaderCircuitBreakerOpenCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "leader_circuit_breaker_open",
		Help:      "The number of times the leader circuit breaker has opened.",
	}

	leaderCircuitBreakerHalfOpenCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "leader_circuit_breaker_half_open",
		Help:      "The number of times the leader circuit breaker has entered half-open state.",
	}

	leaderCircuitBreakerClosedCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "leader_circuit_breaker_closed",
		Help:      "The number of times the leader circuit breaker has closed.",
	}
)

// Metrics contains all the metrics for the endorser
type Metrics struct {
	ProposalDuration         metrics.Histogram
	ProposalsReceived        metrics.Counter
	SuccessfulProposals      metrics.Counter
	ProposalValidationFailed metrics.Counter
	ProposalACLCheckFailed   metrics.Counter
	InitFailed               metrics.Counter
	EndorsementsFailed       metrics.Counter
	DuplicateTxsFailure      metrics.Counter
	SimulationFailure        metrics.Counter

	// New metrics for dependency tracking
	TransactionsWithDependencies metrics.Counter
	DependencyMapSize            metrics.Gauge
	ExpiredDependenciesRemoved   metrics.Counter

	// Circuit breaker metrics
	LeaderCircuitBreakerOpen     metrics.Counter
	LeaderCircuitBreakerHalfOpen metrics.Counter
	LeaderCircuitBreakerClosed   metrics.Counter
}

// NewMetrics creates a new Metrics instance
func NewMetrics(provider metrics.Provider) *Metrics {
	return &Metrics{
		ProposalDuration:         provider.NewHistogram(proposalDurationHistogramOpts),
		ProposalsReceived:        provider.NewCounter(receivedProposalsCounterOpts),
		SuccessfulProposals:      provider.NewCounter(successfulProposalsCounterOpts),
		ProposalValidationFailed: provider.NewCounter(proposalValidationFailureCounterOpts),
		ProposalACLCheckFailed:   provider.NewCounter(proposalChannelACLFailureOpts),
		InitFailed:               provider.NewCounter(initFailureCounterOpts),
		EndorsementsFailed:       provider.NewCounter(endorsementFailureCounterOpts),
		DuplicateTxsFailure:      provider.NewCounter(duplicateTxsFailureCounterOpts),
		SimulationFailure:        provider.NewCounter(simulationFailureCounterOpts),

		// Initialize the new metrics for dependency tracking
		TransactionsWithDependencies: provider.NewCounter(transactionsWithDependenciesCounterOpts),
		DependencyMapSize:            provider.NewGauge(dependencyMapSizeGaugeOpts),
		ExpiredDependenciesRemoved:   provider.NewCounter(expiredDependenciesRemovedCounterOpts),

		// Circuit breaker metrics
		LeaderCircuitBreakerOpen:     provider.NewCounter(leaderCircuitBreakerOpenCounterOpts),
		LeaderCircuitBreakerHalfOpen: provider.NewCounter(leaderCircuitBreakerHalfOpenCounterOpts),
		LeaderCircuitBreakerClosed:   provider.NewCounter(leaderCircuitBreakerClosedCounterOpts),
	}
}
