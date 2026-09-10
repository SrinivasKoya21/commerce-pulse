#!/usr/bin/env bash
# =============================================================================
# CommercePulse — teardown. Deletes EVERYTHING in one shot.
# Run this the moment you finish a working session. Your $200 credit will
# thank you. Re-running azure_setup.sh rebuilds it all in ~10 minutes.
# =============================================================================
set -euo pipefail
RG="rg-commercepulse"

echo "This deletes resource group '$RG' and every resource inside it."
read -r -p "Type 'delete' to confirm: " CONFIRM
[[ "$CONFIRM" == "delete" ]] || { echo "Aborted."; exit 1; }

az group delete --name "$RG" --yes --no-wait
echo "Deletion started (runs in background). Verify later with:"
echo "  az group exists --name $RG    # should print 'false'"

# Databricks-managed resource group (databricks-rg-...) is removed automatically
# with the workspace. Snowflake is a separate trial account — nothing to delete
# there, its credits simply stop being consumed when the warehouse suspends.
