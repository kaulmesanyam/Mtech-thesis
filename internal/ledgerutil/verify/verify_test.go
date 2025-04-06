/*
Copyright Hitachi, Ltd. 2023 All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/internal/ledgerutil/jsonrw"
	"github.com/stretchr/testify/require"
)

const (
	TestDataDir                 = "../testdata/"
	SampleBadLedgerDir          = TestDataDir + "sample_bad_ledger/"
	SampleGoodLedgerDir         = TestDataDir + "sample_prod/"
	SampleResultDir             = TestDataDir + "sample_verifications/"
	SampleLedgerFromSnapshotDir = TestDataDir + "sample_ledger_from_snapshot/"
	VerificationResultFile      = "mychannel_verification_result/blocks.json"
)

func TestVerify(t *testing.T) {
	testCases := []struct {
		name                 string
		sampleFileSystemPath string
		errorExpected        bool
		expectedReturnValue  bool
	}{
		{
			name:                 "ledger-bootstrapped-from-snapshot",
			sampleFileSystemPath: "testdata/ledger-bootstrapped-from-snapshot",
			errorExpected:        false,
			expectedReturnValue:  false,
		},
		{
			name:                 "good-ledger",
			sampleFileSystemPath: SampleGoodLedgerDir,
			errorExpected:        false,
			expectedReturnValue:  true,
		},
		{
			name:                 "hash-error-in-block",
			sampleFileSystemPath: SampleBadLedgerDir,
			errorExpected:        false,
			expectedReturnValue:  false,
		},
		{
			name:                 "block-store-does-not-exist",
			sampleFileSystemPath: "testdata/block-store-does-not-exist",
			errorExpected:        true,
			expectedReturnValue:  false,
		},
		{
			name:                 "empty-block-store",
			sampleFileSystemPath: "testdata/empty-block-store",
			errorExpected:        true,
			expectedReturnValue:  false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testName := testCase.name
			fsDir := filepath.Join(TestDataDir, testName)
			outputDir := filepath.Join(TestDataDir, testName+"-output")
			err := os.MkdirAll(fsDir, 0o700)
			require.NoError(t, err)
			err = os.MkdirAll(outputDir, 0o700)
			require.NoError(t, err)

			if testName == "empty-block-store" {
				err := os.MkdirAll(filepath.Join(fsDir, "ledgersData", "chains"), 0o700)
				require.NoError(t, err)
			} else if testName != "block-store-does-not-exist" {
				err := testutil.CopyDir(testCase.sampleFileSystemPath, fsDir, false)
				require.NoError(t, err)
			}

			anyError, err := VerifyLedger(fsDir, outputDir)

			if testCase.errorExpected {
				require.Error(t, err)

				if testName == "empty-block-store" {
					require.ErrorContains(t, err, fmt.Sprintf("provided path %s is empty. Aborting verify", fsDir))
				} else {
					require.ErrorContains(t, err, fmt.Sprintf("open %s: no such file or directory", filepath.Join(fsDir, "ledgersData", "chains")))
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, anyError, testCase.expectedReturnValue)

				actualResult, err := jsonrw.OutputFileToString(VerificationResultFile, outputDir)
				require.NoError(t, err)
				expectedResult, err := jsonrw.OutputFileToString(testCase.name+".json", SampleResultDir)
				require.NoError(t, err)

				require.Equal(t, actualResult, expectedResult)
			}
		})
	}
}
