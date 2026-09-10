#!/usr/bin/env bash
# =============================================================================
# CommercePulse — Azure infrastructure bootstrap
# Provisions: resource group, ADLS Gen2, Event Hubs (Kafka-enabled), Databricks
# Cost: designed to fit comfortably in the $200 Azure free trial.
# Run:   bash infra/azure_setup.sh
# Undo:  bash infra/teardown.sh
# =============================================================================
set -euo pipefail

# ----------------------------- configuration --------------------------------
export LOCATION="eastus2"                       # cheap + most services available
export SUFFIX=$(( RANDOM % 9000 + 1000 ))        # storage names must be global-unique
export RG="rg-commercepulse"
export STORAGE="cpdatalake${SUFFIX}"             # lowercase, no dashes
export EH_NAMESPACE="cp-events-${SUFFIX}"
export EH_CLICKSTREAM="clickstream"
export EH_ORDERS="orders"
export DBX_WORKSPACE="cp-databricks"

echo ">>> Using suffix ${SUFFIX} — SAVE THIS. Resource names depend on it."
echo "SUFFIX=${SUFFIX}" > infra/.env.generated

# ----------------------------- resource group -------------------------------
az group create --name "$RG" --location "$LOCATION" -o table

# ----------------------------- ADLS Gen2 ------------------------------------
# Hierarchical namespace = true makes it a real data lake (directories + ACLs).
az storage account create \
  --name "$STORAGE" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  -o table

STORAGE_KEY=$(az storage account keys list -g "$RG" -n "$STORAGE" --query "[0].value" -o tsv)

# Medallion containers
for c in bronze silver gold checkpoints raw; do
  az storage container create --name "$c" \
    --account-name "$STORAGE" --account-key "$STORAGE_KEY" -o none
  echo "  container: $c"
done

# ----------------------------- Event Hubs (Kafka) ----------------------------
# Standard tier is REQUIRED for the Kafka protocol head.
az eventhubs namespace create \
  --name "$EH_NAMESPACE" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku Standard \
  --capacity 1 \
  -o table

# One hub per topic. partition-count 4 lets Spark parallelize reads.
for hub in "$EH_CLICKSTREAM" "$EH_ORDERS"; do
  az eventhubs eventhub create \
    --name "$hub" \
    --namespace-name "$EH_NAMESPACE" \
    --resource-group "$RG" \
    --partition-count 4 \
    --cleanup-policy Delete \
    --retention-time 24 \
    -o none
  echo "  event hub (kafka topic): $hub"
done

EH_CONN=$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$RG" --namespace-name "$EH_NAMESPACE" \
  --name RootManageSharedAccessKey --query primaryConnectionString -o tsv)

# ----------------------------- Databricks ------------------------------------
az extension add --name databricks --only-show-errors || true
az databricks workspace create \
  --name "$DBX_WORKSPACE" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku premium \
  -o table
# premium SKU enables Unity Catalog + 14-day free DBU trial on new subscriptions

# ----------------------------- output ----------------------------------------
cat <<EOF | tee infra/.env.local

============================================================
 SAVE THESE VALUES (infra/.env.local) — never commit them
============================================================
STORAGE_ACCOUNT=${STORAGE}
STORAGE_KEY=${STORAGE_KEY}
EH_NAMESPACE=${EH_NAMESPACE}
EH_BOOTSTRAP=${EH_NAMESPACE}.servicebus.windows.net:9093
EH_CONNECTION_STRING=${EH_CONN}
EOF
echo "infra/.env.local written. Add it to .gitignore (already done)."
