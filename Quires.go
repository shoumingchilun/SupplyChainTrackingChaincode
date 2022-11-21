package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// QueryResult structure used for handling result of query
type QueryResult struct {
	Record    *Commodity
	TxId      string    `json:"txId"`
	Timestamp time.Time `json:"timestamp"`
}

type Agreement struct {
	ID          string `json:"commodity"`
	TransferKey int    `json:"transferKey"`
	TransferID  string `json:"transfer_id"`
}

// ReadCommodity returns the public commodity data
func (s *SmartContract) ReadCommodity(ctx contractapi.TransactionContextInterface, commodityID string) (*Commodity, error) {
	// Since only public data is accessed in this function, no access control is required
	commodityJSON, err := ctx.GetStub().GetState(commodityID)
	if err != nil {
		return nil, fmt.Errorf("failed to read from world state: %v", err)
	}
	if commodityJSON == nil {
		return nil, fmt.Errorf("%s does not exist", commodityID)
	}

	var commodity *Commodity
	err = json.Unmarshal(commodityJSON, &commodity)
	if err != nil {
		return nil, err
	}
	return commodity, nil
}

// GetCommodityPrivateProperties returns the immutable commodity properties from owner's private data collection
func (s *SmartContract) GetCommodityPrivateProperties(ctx contractapi.TransactionContextInterface, commodityID string) (string, error) {

	collection, err := getClientImplicitCollectionNameAndVerifyClientOrg(ctx)
	if err != nil {
		return "", err
	}

	immutableProperties, err := ctx.GetStub().GetPrivateData(collection, commodityID)
	if err != nil {
		return "", fmt.Errorf("failed to read commodity private properties from client org's collection: %v", err)
	}
	if immutableProperties == nil {
		return "", fmt.Errorf("commodity private details does not exist in client org's collection: %s", commodityID)
	}

	return string(immutableProperties), nil
}

// GetCommodityUpstreamKey returns the Upstream company's transferKey
func (s *SmartContract) GetCommodityUpstreamKey(ctx contractapi.TransactionContextInterface, commodityID string) (string, error) {
	return getTransferKey(ctx, commodityID, typeCommodityForTransfer)
}

// GetCommodityDownstreamKey returns the Downstream company's transferKey
func (s *SmartContract) GetCommodityDownstreamKey(ctx contractapi.TransactionContextInterface, commodityID string) (string, error) {
	return getTransferKey(ctx, commodityID, typeCommodityKey)
}

// getTransferKey gets the bid or ask price from caller's implicit private data collection
func getTransferKey(ctx contractapi.TransactionContextInterface, commodityID string, keyType string) (string, error) {

	collection, err := getClientImplicitCollectionNameAndVerifyClientOrg(ctx)
	if err != nil {
		return "", err
	}

	commodityTransferKey, err := ctx.GetStub().CreateCompositeKey(keyType, []string{commodityID})
	if err != nil {
		return "", fmt.Errorf("failed to create composite key: %v", err)
	}

	key, err := ctx.GetStub().GetPrivateData(collection, commodityTransferKey)
	if err != nil {
		return "", fmt.Errorf("failed to read commodity key from implicit private data collection: %v", err)
	}
	if key == nil {
		return "", fmt.Errorf("commodity key does not exist: %s", commodityID)
	}

	return string(key), nil
}

// QueryCommodityPutAgreements returns all of an organization's proposed Putting
func (s *SmartContract) QueryCommodityPutAgreements(ctx contractapi.TransactionContextInterface) ([]Agreement, error) {
	return queryAgreementsByType(ctx, typeCommodityForTransfer)
}

// QueryCommodityGetAgreements returns all of an organization's proposed Getting
func (s *SmartContract) QueryCommodityGetAgreements(ctx contractapi.TransactionContextInterface) ([]Agreement, error) {
	return queryAgreementsByType(ctx, typeCommodityKey)
}

func queryAgreementsByType(ctx contractapi.TransactionContextInterface, agreeType string) ([]Agreement, error) {
	collection, err := getClientImplicitCollectionNameAndVerifyClientOrg(ctx)
	if err != nil {
		return nil, err
	}

	// Query for any object type starting with `agreeType`
	agreementsIterator, err := ctx.GetStub().GetPrivateDataByPartialCompositeKey(collection, agreeType, []string{})
	if err != nil {
		return nil, fmt.Errorf("failed to read from private data collection: %v", err)
	}
	defer agreementsIterator.Close()

	var agreements []Agreement
	for agreementsIterator.HasNext() {
		resp, err := agreementsIterator.Next()
		if err != nil {
			return nil, err
		}

		var agreement Agreement
		err = json.Unmarshal(resp.Value, &agreement)
		if err != nil {
			return nil, err
		}

		agreements = append(agreements, agreement)
	}

	return agreements, nil
}

// QueryCommodityHistory returns the chain of custody for a commodity since issuance
func (s *SmartContract) QueryCommodityHistory(ctx contractapi.TransactionContextInterface, assetID string) ([]QueryResult, error) {
	resultsIterator, err := ctx.GetStub().GetHistoryForKey(assetID)
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var results []QueryResult
	for resultsIterator.HasNext() {
		response, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		var commodity *Commodity
		err = json.Unmarshal(response.Value, &commodity)
		if err != nil {
			return nil, err
		}

		timestamp, err := ptypes.Timestamp(response.Timestamp)
		if err != nil {
			return nil, err
		}
		record := QueryResult{
			TxId:      response.TxId,
			Timestamp: timestamp,
			Record:    commodity,
		}
		results = append(results, record)
	}

	return results, nil
}
