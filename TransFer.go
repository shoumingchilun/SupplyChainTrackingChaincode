package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/hyperledger/fabric-chaincode-go/pkg/statebased"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	"log"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

const (
	typeCommodityForTransfer = "T"
	typeCommodityKey         = "K"
	typeCommodityPutReceipt  = "PR"
	typeCommodityGetReceipt  = "GR"
)

type SmartContract struct {
	contractapi.Contract
}

// Commodity struct and properties must be exported (start with capitals) to work with contract api metadata
type Commodity struct {
	ObjectType          string `json:"objectType"` // ObjectType is used to distinguish different object types in the same chaincode namespace
	ID                  string `json:"commodityID"`
	OwnerOrg            string `json:"ownerCompany"`
	Source              string `json:"source"`
	Target              string `json:"target"`
	PublicDescription   string `json:"publicDescription"`
	DetailedInformation string `json:"detailedInformation"`
}
type receipt struct {
	transferKey int
	timestamp   time.Time
}

// CreateAsset creates a Commodity, sets it as owned by the client's org and returns its id
// the id of the commodity corresponds to the hash of the properties of the commodity that are  passed by transient field
func (s *SmartContract) CreateAsset(ctx contractapi.TransactionContextInterface, target string, publicDescription string) (string, error) {
	transientMap, err := ctx.GetStub().GetTransient()
	if err != nil {
		return "", fmt.Errorf("error getting transient: %v", err)
	}

	// Commodity properties must be retrieved from the transient field as they are private
	immutablePropertiesJSON, ok := transientMap["commodity_properties"]
	if !ok {
		return "", fmt.Errorf("commodity_properties key not found in the transient map")
	}

	// CommodityID will be the hash of the commodity's properties
	hash := sha256.New()
	hash.Write(immutablePropertiesJSON)
	commodityID := hex.EncodeToString(hash.Sum(nil))

	// Get the clientOrgId from the input, will be used for implicit collection, owner, and state-based endorsement policy
	clientOrgID, err := getClientOrgID(ctx)
	if err != nil {
		return "", err
	}

	// In the test, client is only authorized to read/write private data from its own peer, therefore verify client org id matches peer org id.
	err = verifyClientOrgMatchesPeerOrg(clientOrgID)
	if err != nil {
		return "", err
	}

	commodity := Commodity{
		ObjectType:        "Commodity",
		ID:                commodityID,
		OwnerOrg:          clientOrgID,
		Source:            clientOrgID,
		Target:            target,
		PublicDescription: publicDescription,
	}
	commodityBytes, err := json.Marshal(commodity)
	if err != nil {
		return "", fmt.Errorf("failed to create commodity JSON: %v", err)
	}

	err = ctx.GetStub().PutState(commodityID, commodityBytes)
	if err != nil {
		return "", fmt.Errorf("failed to put commodity in public data: %v", err)
	}

	// Set the endorsement policy such that an owner org peer is required to endorse future updates.
	// Actually, consider additional endorsers such as a trusted third party to further secure transfers.
	endorsingOrges := []string{clientOrgID}
	err = setCommodityStateBasedEndorsement(ctx, commodity.ID, endorsingOrges)
	if err != nil {
		return "", fmt.Errorf("failed setting state based endorsement for upstream and downstream companies: %v", err)
	}

	// Persist private immutable commodity properties to owner's private data collection
	collection := buildCollectionName(clientOrgID)
	err = ctx.GetStub().PutPrivateData(collection, commodityID, immutablePropertiesJSON)
	if err != nil {
		return "", fmt.Errorf("failed to put Commodity private details: %v", err)
	}

	return commodityID, nil
}

// ChangePublicDescription updates the assets public description. Only the current owner can update the public description
func (s *SmartContract) ChangePublicDescription(ctx contractapi.TransactionContextInterface, commodityID string, newDescription string) error {

	clientOrgID, err := getClientOrgID(ctx)
	if err != nil {
		return err
	}

	commodity, err := s.ReadCommodity(ctx, commodityID)
	if err != nil {
		return fmt.Errorf("failed to get commodity: %v", err)
	}

	// Auth check to ensure that client's org actually owns the commodity
	if clientOrgID != commodity.OwnerOrg {
		return fmt.Errorf("a client from %s cannot update the description of a commodity owned by %s", clientOrgID, commodity.OwnerOrg)
	}

	commodity.PublicDescription = newDescription
	updatedAssetJSON, err := json.Marshal(commodity)
	if err != nil {
		return fmt.Errorf("failed to marshal commodity: %v", err)
	}

	return ctx.GetStub().PutState(commodityID, updatedAssetJSON)
}

// AgreeToPut adds upstream company's TransferKey and Commodity its implicit private data collection.
func (s *SmartContract) AgreeToPut(ctx contractapi.TransactionContextInterface, commodityID string) error {
	asset, err := s.ReadCommodity(ctx, commodityID)
	if err != nil {
		return err
	}

	clientOrgID, err := getClientOrgID(ctx)
	if err != nil {
		return err
	}

	// Verify that this client belongs to the peer's org
	err = verifyClientOrgMatchesPeerOrg(clientOrgID)
	if err != nil {
		return err
	}

	// Verify that this clientOrgId actually owns the commodity.
	if clientOrgID != asset.OwnerOrg {
		return fmt.Errorf("a client from %s cannot update a commodity owned by %s", clientOrgID, asset.OwnerOrg)
	}

	return agreeToTransfer(ctx, commodityID, typeCommodityForTransfer)
}

// AgreeToGet adds downstream company's transferKey and Commodity to its implicit private data collection
func (s *SmartContract) AgreeToGet(ctx contractapi.TransactionContextInterface, CommodityID string) error {
	transientMap, err := ctx.GetStub().GetTransient()
	if err != nil {
		return fmt.Errorf("error getting transient: %v", err)
	}

	clientOrgID, err := getClientOrgID(ctx)
	if err != nil {
		return err
	}

	// Verify that this client belongs to the peer's org
	err = verifyClientOrgMatchesPeerOrg(clientOrgID)
	if err != nil {
		return err
	}

	// Commodity properties must be retrieved from the transient field as they are private
	immutablePropertiesJSON, ok := transientMap["Commodity_properties"]
	if !ok {
		return fmt.Errorf("commodity_properties key not found in the transient map")
	}

	// Persist private immutable asset properties to seller's private data collection
	collection := buildCollectionName(clientOrgID)
	err = ctx.GetStub().PutPrivateData(collection, CommodityID, immutablePropertiesJSON)
	if err != nil {
		return fmt.Errorf("failed to put Asset private details: %v", err)
	}

	return agreeToTransfer(ctx, CommodityID, typeCommodityKey)
}

// agreeToTransfer adds a transferKey to caller's implicit private data collection
func agreeToTransfer(ctx contractapi.TransactionContextInterface, commodityID string, transferType string) error {
	// In this scenario, both Upstream and downstream companies are authored to read/write private about transfer after Upstream agrees to put.
	clientOrgID, err := getClientOrgID(ctx)
	if err != nil {
		return err
	}

	transMap, err := ctx.GetStub().GetTransient()
	if err != nil {
		return fmt.Errorf("error getting transient: %v", err)
	}

	// Asset transferKey must be retrieved from the transient field as they are private
	transferKey, ok := transMap["commodity_transferKey"]
	if !ok {
		return fmt.Errorf("commodity_transferKey not found in the transient map")
	}

	collection := buildCollectionName(clientOrgID)

	// Persist the agreed to transferKey in a collection sub-namespace based on commodity_transferKey prefix,
	// to avoid collisions between private commodity properties and transferKeys
	commodityPriceKey, err := ctx.GetStub().CreateCompositeKey(transferType, []string{commodityID})
	if err != nil {
		return fmt.Errorf("failed to create composite key: %v", err)
	}

	// The TransferKey hash will be verified later, therefore always pass and persist transferKey bytes as is,
	// so that there is no risk of nondeterministic marshaling.
	err = ctx.GetStub().PutPrivateData(collection, commodityPriceKey, transferKey)
	if err != nil {
		return fmt.Errorf("failed to put transferKey: %v", err)
	}

	return nil
}

// VerifyCommodityProperties allows an upstream company to validate the properties of
// a commodity they intend to get from the owner's implicit private data collection
// and verifies that the commodity properties never changed from the origin of the commodity by checking their hash against the commodityID
func (s *SmartContract) VerifyCommodityProperties(ctx contractapi.TransactionContextInterface, commodityID string) (bool, error) {
	transMap, err := ctx.GetStub().GetTransient()
	if err != nil {
		return false, fmt.Errorf("error getting transient: %v", err)
	}

	// Commodity properties must be retrieved from the transient field as they are private
	immutablePropertiesJSON, ok := transMap["Commodity_properties"]
	if !ok {
		return false, fmt.Errorf("commodity_properties key not found in the transient map")
	}

	commodity, err := s.ReadCommodity(ctx, commodityID)
	if err != nil {
		return false, fmt.Errorf("failed to get commodity: %v", err)
	}

	collectionOwner := buildCollectionName(commodity.OwnerOrg)
	immutablePropertiesOnChainHash, err := ctx.GetStub().GetPrivateDataHash(collectionOwner, commodityID)
	if err != nil {
		return false, fmt.Errorf("failed to read commodity private properties hash from upstream company's collection: %v", err)
	}
	if immutablePropertiesOnChainHash == nil {
		return false, fmt.Errorf("commodity private properties hash does not exist: %s", commodityID)
	}

	hash := sha256.New()
	hash.Write(immutablePropertiesJSON)
	calculatedPropertiesHash := hash.Sum(nil)

	// verify that the hash of the passed immutable properties matches the on-chain hash
	if !bytes.Equal(immutablePropertiesOnChainHash, calculatedPropertiesHash) {
		return false, fmt.Errorf("hash %x for passed immutable properties %s does not match on-chain hash %x",
			calculatedPropertiesHash,
			immutablePropertiesJSON,
			immutablePropertiesOnChainHash,
		)
	}

	// verify that the hash of the passed immutable properties and on chain hash matches the commodityID
	if !(hex.EncodeToString(immutablePropertiesOnChainHash) == commodityID) {
		return false, fmt.Errorf("hash %x for passed immutable properties %s does match on-chain hash %x but do not match commodityID %s: commodity was altered from its initial form",
			calculatedPropertiesHash,
			immutablePropertiesJSON,
			immutablePropertiesOnChainHash,
			commodityID)
	}

	return true, nil
}

// TransferCommodity checks transfer conditions and then transfers commodity state to buyer.
// TransferCommodity can only be called by current owner
func (s *SmartContract) TransferCommodity(ctx contractapi.TransactionContextInterface, commodityID string, downStreamOrgID string) error {
	clientOrgID, err := getClientOrgID(ctx)
	if err != nil {
		return err
	}

	transMap, err := ctx.GetStub().GetTransient()
	if err != nil {
		return fmt.Errorf("error getting transient data: %v", err)
	}

	transferKeyJSON, ok := transMap["Commodity_transferKey"]
	if !ok {
		return fmt.Errorf("commodity_transferKey key not found in the transient map")
	}

	var agreement Agreement
	err = json.Unmarshal(transferKeyJSON, &agreement)
	if err != nil {
		return fmt.Errorf("failed to unmarshal price JSON: %v", err)
	}

	commodity, err := s.ReadCommodity(ctx, commodityID)
	if err != nil {
		return fmt.Errorf("failed to get commodity: %v", err)
	}

	err = verifyTransferConditions(ctx, commodity, clientOrgID, downStreamOrgID, transferKeyJSON)
	if err != nil {
		return fmt.Errorf("failed transfer verification: %v", err)
	}

	err = transferCommodityState(ctx, commodity, clientOrgID, downStreamOrgID, agreement.TransferKey)
	if err != nil {
		return fmt.Errorf("failed commodity transfer: %v", err)
	}

	return nil

}

// verifyTransferConditions checks that client org currently owns commodity and that both parties have agreed on transferKay
func verifyTransferConditions(ctx contractapi.TransactionContextInterface,
	commodity *Commodity,
	clientOrgID string,
	upstreamOrgID string,
	transferKayJSON []byte) error {

	// CHECK1: Auth check to ensure that client's org actually owns the commodity

	if clientOrgID != commodity.OwnerOrg {
		return fmt.Errorf("a client from %s cannot transfer a commodity owned by %s", clientOrgID, commodity.OwnerOrg)
	}

	// CHECK2: Verify that upstream and downstream companies on-chain commodity definition hash matches

	collectionPutter := buildCollectionName(clientOrgID)
	collectionGetter := buildCollectionName(upstreamOrgID)
	ownerPropertiesOnChainHash, err := ctx.GetStub().GetPrivateDataHash(collectionPutter, commodity.ID)
	if err != nil {
		return fmt.Errorf("failed to read commodity private properties hash from Putter's collection: %v", err)
	}
	if ownerPropertiesOnChainHash == nil {
		return fmt.Errorf("commodity private properties hash does not exist: %s", commodity.ID)
	}
	GetterPropertiesOnChainHash, err := ctx.GetStub().GetPrivateDataHash(collectionGetter, commodity.ID)
	if err != nil {
		return fmt.Errorf("failed to read commodity private properties hash from Getter's collection: %v", err)
	}
	if GetterPropertiesOnChainHash == nil {
		return fmt.Errorf("commodity private properties hash does not exist: %s", commodity.ID)
	}

	// verify that upstream and downstream companies on-chain commodity definition hash matches
	if !bytes.Equal(ownerPropertiesOnChainHash, GetterPropertiesOnChainHash) {
		return fmt.Errorf("on chain hash of seller %x does not match on-chain hash of buyer %x",
			ownerPropertiesOnChainHash,
			GetterPropertiesOnChainHash,
		)
	}

	// CHECK3: Verify that upstream and downstream companies have the same transferKey

	// Get upstream company's transferKay
	commodityForPutKey, err := ctx.GetStub().CreateCompositeKey(typeCommodityForTransfer, []string{commodity.ID})
	if err != nil {
		return fmt.Errorf("failed to create composite key: %v", err)
	}
	upstreamTransferKeyHash, err := ctx.GetStub().GetPrivateDataHash(collectionPutter, commodityForPutKey)
	if err != nil {
		return fmt.Errorf("failed to get upstream company's transferKay's hash: %v", err)
	}
	if upstreamTransferKeyHash == nil {
		return fmt.Errorf("upstream company's transferKay for %s does not exist", commodity.ID)
	}

	// Get downstream company's transferKay
	commodityForGetKey, err := ctx.GetStub().CreateCompositeKey(typeCommodityKey, []string{commodity.ID})
	if err != nil {
		return fmt.Errorf("failed to create composite key: %v", err)
	}
	downstreamTransferKeyHash, err := ctx.GetStub().GetPrivateDataHash(collectionGetter, commodityForGetKey)
	if err != nil {
		return fmt.Errorf("failed to get downstream company's transferKay's hash: %v", err)
	}
	if downstreamTransferKeyHash == nil {
		return fmt.Errorf("downstream company's transferKay for %s does not exist", commodity.ID)
	}

	hash := sha256.New()
	hash.Write(transferKayJSON)
	calculatedKeyHash := hash.Sum(nil)

	// Verify that the hash of the key matches the on-chain upstream company's key hash
	if !bytes.Equal(calculatedKeyHash, upstreamTransferKeyHash) {
		return fmt.Errorf("hash %x for passed key JSON %s does not match on-chain hash %x, wrong trade id and tranferKey with upstream company's",
			calculatedKeyHash,
			transferKayJSON,
			upstreamTransferKeyHash,
		)
	}

	// Verify that the hash of the passed key matches the on-chain downstream company's key
	if !bytes.Equal(calculatedKeyHash, downstreamTransferKeyHash) {
		return fmt.Errorf("hash %x for passed key JSON %s does not match on-chain hash %x, wrong trade id and tranferKey with downstream company's",
			calculatedKeyHash,
			transferKayJSON,
			downstreamTransferKeyHash,
		)
	}

	return nil
}

// transferCommodityState performs the public and private state updates for the transferred commodity
// changes the endorsement for the transferred commodity sbe to the new owner org
// save the old owner as source
func transferCommodityState(ctx contractapi.TransactionContextInterface, commodity *Commodity, clientOrgID string, upstreamOrgID string, transferKey int) error {

	// Update ownership and source in public state
	commodity.Source = commodity.OwnerOrg
	commodity.OwnerOrg = upstreamOrgID

	updatedCommodity, err := json.Marshal(commodity)
	if err != nil {
		return err
	}
	err = ctx.GetStub().PutState(commodity.ID, updatedCommodity)
	if err != nil {
		return fmt.Errorf("failed to write commodity for upstream: %v", err)
	}

	// Changes the endorsement policy to the new owner org
	endorsingOrges := []string{upstreamOrgID}
	err = setCommodityStateBasedEndorsement(ctx, commodity.ID, endorsingOrges)
	if err != nil {
		return fmt.Errorf("failed setting state based endorsement for new owner: %v", err)
	}

	// Delete commodity description from upstream collection
	collectionPutter := buildCollectionName(clientOrgID)
	err = ctx.GetStub().DelPrivateData(collectionPutter, commodity.ID)
	if err != nil {
		return fmt.Errorf("failed to delete commodity private details from upstream: %v", err)
	}

	// Delete the transferKey records for upstream
	commodityTransferKey, err := ctx.GetStub().CreateCompositeKey(typeCommodityForTransfer, []string{commodity.ID})
	if err != nil {
		return fmt.Errorf("failed to create composite key for upstream: %v", err)
	}
	err = ctx.GetStub().DelPrivateData(collectionPutter, commodityTransferKey)
	if err != nil {
		return fmt.Errorf("failed to delete commodity transferKey from implicit private data collection for Putter: %v", err)
	}

	// Delete the transferKey records for Getter
	collectionGetter := buildCollectionName(upstreamOrgID)
	commodityTransferKey, err = ctx.GetStub().CreateCompositeKey(typeCommodityKey, []string{commodity.ID})
	if err != nil {
		return fmt.Errorf("failed to create composite key for Getter: %v", err)
	}
	err = ctx.GetStub().DelPrivateData(collectionGetter, commodityTransferKey)
	if err != nil {
		return fmt.Errorf("failed to delete commodity transferKey from implicit private data collection for Getter: %v", err)
	}

	// Keep record for a 'receipt' in both upstream and downstream companies' private data collection to record the sale transferKey and date.
	// Persist the agreed to transferKey in a collection sub-namespace based on receipt key prefix.
	receiptGetKey, err := ctx.GetStub().CreateCompositeKey(typeCommodityGetReceipt, []string{commodity.ID, ctx.GetStub().GetTxID()})
	if err != nil {
		return fmt.Errorf("failed to create composite key for receipt: %v", err)
	}

	txTimestamp, err := ctx.GetStub().GetTxTimestamp()
	if err != nil {
		return fmt.Errorf("failed to create timestamp for receipt: %v", err)
	}

	timestamp, err := ptypes.Timestamp(txTimestamp)
	if err != nil {
		return err
	}
	commodityReceipt := receipt{
		transferKey: transferKey,
		timestamp:   timestamp,
	}
	receipt, err := json.Marshal(commodityReceipt)
	if err != nil {
		return fmt.Errorf("failed to marshal receipt: %v", err)
	}

	err = ctx.GetStub().PutPrivateData(collectionGetter, receiptGetKey, receipt)
	if err != nil {
		return fmt.Errorf("failed to put private commodity receipt for Getter: %v", err)
	}

	receiptPutKey, err := ctx.GetStub().CreateCompositeKey(typeCommodityPutReceipt, []string{ctx.GetStub().GetTxID(), commodity.ID})
	if err != nil {
		return fmt.Errorf("failed to create composite key for receipt: %v", err)
	}

	err = ctx.GetStub().PutPrivateData(collectionPutter, receiptPutKey, receipt)
	if err != nil {
		return fmt.Errorf("failed to put private commodity receipt for Putter: %v", err)
	}

	return nil
}

// getClientOrgID gets the client org ID.
func getClientOrgID(ctx contractapi.TransactionContextInterface) (string, error) {
	clientOrgID, err := ctx.GetClientIdentity().GetMSPID()
	if err != nil {
		return "", fmt.Errorf("failed getting client's orgID: %v", err)
	}

	return clientOrgID, nil
}

// getClientImplicitCollectionNameAndVerifyClientOrg gets the implicit collection for the client and checks that the client is from the same org as the peer
func getClientImplicitCollectionNameAndVerifyClientOrg(ctx contractapi.TransactionContextInterface) (string, error) {
	clientOrgID, err := getClientOrgID(ctx)
	if err != nil {
		return "", err
	}

	err = verifyClientOrgMatchesPeerOrg(clientOrgID)
	if err != nil {
		return "", err
	}

	return buildCollectionName(clientOrgID), nil
}

// verifyClientOrgMatchesPeerOrg checks that the client is from the same org as the peer
func verifyClientOrgMatchesPeerOrg(clientOrgID string) error {
	peerOrgID, err := shim.GetMSPID()
	if err != nil {
		return fmt.Errorf("failed getting peer's orgID: %v", err)
	}

	if clientOrgID != peerOrgID {
		return fmt.Errorf("client from org %s is not authorized to read or write private data from an org %s peer",
			clientOrgID,
			peerOrgID,
		)
	}

	return nil
}

// buildCollectionName returns the implicit collection name for an org
func buildCollectionName(clientOrgID string) string {
	return fmt.Sprintf("_implicit_org_%s", clientOrgID)
}

// setCommodityStateBasedEndorsement adds an endorsement policy to an asset so that the passed orges need to agree upon transfer
func setCommodityStateBasedEndorsement(ctx contractapi.TransactionContextInterface, assetID string, orgesToEndorse []string) error {
	endorsementPolicy, err := statebased.NewStateEP(nil)
	if err != nil {
		return err
	}
	err = endorsementPolicy.AddOrgs(statebased.RoleTypePeer, orgesToEndorse...)
	if err != nil {
		return fmt.Errorf("failed to add org to endorsement policy: %v", err)
	}
	policy, err := endorsementPolicy.Policy()
	if err != nil {
		return fmt.Errorf("failed to create endorsement policy bytes from org: %v", err)
	}
	err = ctx.GetStub().SetStateValidationParameter(assetID, policy)
	if err != nil {
		return fmt.Errorf("failed to set validation parameter on asset: %v", err)
	}

	return nil
}

// GetCommodityHashId allows a potential downstream to validate the properties of a commodity against the commodityId hash on chain and returns the hash
func (s *SmartContract) GetCommodityHashId(ctx contractapi.TransactionContextInterface) (string, error) {
	transientMap, err := ctx.GetStub().GetTransient()
	if err != nil {
		return "", fmt.Errorf("error getting transient: %v", err)
	}

	// Asset properties must be retrieved from the transient field as they are private
	propertiesJSON, ok := transientMap["Commodity_properties"]
	if !ok {
		return "", fmt.Errorf("commodity_properties key not found in the transient map")
	}

	hash := sha256.New()
	hash.Write(propertiesJSON)
	commodityID := hex.EncodeToString(hash.Sum(nil))

	commodity, err := s.ReadCommodity(ctx, commodityID)
	if err != nil {
		return "", fmt.Errorf("failed to get commodity: %v, commodity properies provided do not represent any on chain commodity", err)
	}
	if commodity.ID != commodityID {
		return "", fmt.Errorf("commodity properies provided do not correpond to any on chain commodity")
	}
	return commodity.ID, nil
}

func main() {
	chaincode, err := contractapi.NewChaincode(new(SmartContract))
	if err != nil {
		log.Panicf("Error create transfer asset chaincode: %v", err)
	}

	if err := chaincode.Start(); err != nil {
		log.Panicf("Error starting asset chaincode: %v", err)
	}
}
