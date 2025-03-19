// Code generated by counterfeiter. DO NOT EDIT.
package mock

import (
	"sync"
)

type ACLProvider struct {
	CheckACLNoChannelStub        func(string, interface{}) error
	checkACLNoChannelMutex       sync.RWMutex
	checkACLNoChannelArgsForCall []struct {
		arg1 string
		arg2 interface{}
	}
	checkACLNoChannelReturns struct {
		result1 error
	}
	checkACLNoChannelReturnsOnCall map[int]struct {
		result1 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *ACLProvider) CheckACLNoChannel(arg1 string, arg2 interface{}) error {
	fake.checkACLNoChannelMutex.Lock()
	ret, specificReturn := fake.checkACLNoChannelReturnsOnCall[len(fake.checkACLNoChannelArgsForCall)]
	fake.checkACLNoChannelArgsForCall = append(fake.checkACLNoChannelArgsForCall, struct {
		arg1 string
		arg2 interface{}
	}{arg1, arg2})
	fake.recordInvocation("CheckACLNoChannel", []interface{}{arg1, arg2})
	fake.checkACLNoChannelMutex.Unlock()
	if fake.CheckACLNoChannelStub != nil {
		return fake.CheckACLNoChannelStub(arg1, arg2)
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.checkACLNoChannelReturns
	return fakeReturns.result1
}

func (fake *ACLProvider) CheckACLNoChannelCallCount() int {
	fake.checkACLNoChannelMutex.RLock()
	defer fake.checkACLNoChannelMutex.RUnlock()
	return len(fake.checkACLNoChannelArgsForCall)
}

func (fake *ACLProvider) CheckACLNoChannelCalls(stub func(string, interface{}) error) {
	fake.checkACLNoChannelMutex.Lock()
	defer fake.checkACLNoChannelMutex.Unlock()
	fake.CheckACLNoChannelStub = stub
}

func (fake *ACLProvider) CheckACLNoChannelArgsForCall(i int) (string, interface{}) {
	fake.checkACLNoChannelMutex.RLock()
	defer fake.checkACLNoChannelMutex.RUnlock()
	argsForCall := fake.checkACLNoChannelArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *ACLProvider) CheckACLNoChannelReturns(result1 error) {
	fake.checkACLNoChannelMutex.Lock()
	defer fake.checkACLNoChannelMutex.Unlock()
	fake.CheckACLNoChannelStub = nil
	fake.checkACLNoChannelReturns = struct {
		result1 error
	}{result1}
}

func (fake *ACLProvider) CheckACLNoChannelReturnsOnCall(i int, result1 error) {
	fake.checkACLNoChannelMutex.Lock()
	defer fake.checkACLNoChannelMutex.Unlock()
	fake.CheckACLNoChannelStub = nil
	if fake.checkACLNoChannelReturnsOnCall == nil {
		fake.checkACLNoChannelReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.checkACLNoChannelReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *ACLProvider) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.checkACLNoChannelMutex.RLock()
	defer fake.checkACLNoChannelMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *ACLProvider) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}
