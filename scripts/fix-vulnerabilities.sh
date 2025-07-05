#!/bin/bash

# Script to fix known vulnerabilities in synthetics-monitor-extractor
# This script addresses specific security issues and provides automated fixes

set -euo pipefail

echo "🔧 Fixing Known Vulnerabilities"
echo "================================="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    
    case $status in
        "error")
            echo -e "${RED}✗ ${message}${NC}"
            ;;
        "success")
            echo -e "${GREEN}✓ ${message}${NC}"
            ;;
        "warning")
            echo -e "${YELLOW}⚠ ${message}${NC}"
            ;;
        "info")
            echo -e "${BLUE}ℹ ${message}${NC}"
            ;;
    esac
}

echo "1. Fixing lodash vulnerabilities..."
echo "-----------------------------------"

# Check if package.json has overrides
if grep -q "overrides" package.json; then
    print_status "success" "Package overrides are configured"
else
    print_status "info" "Adding package overrides to force secure versions"
    
    # Add overrides section to package.json if it doesn't exist
    if ! grep -q "\"overrides\":" package.json; then
        # Use a temporary file to add overrides
        cp package.json package.json.bak
        
        # Insert overrides before the closing brace
        sed 's/^}$/  "overrides": {\
    "lodash": "^4.17.21",\
    "inquirer": "^9.2.15"\
  }\
}/' package.json.bak > package.json
        
        rm package.json.bak
        print_status "success" "Added package overrides"
    fi
fi

echo ""
echo "2. Updating dependencies..."
echo "---------------------------"

# Update all dependencies
print_status "info" "Running bun update..."
bun update

# Reinstall to apply overrides
print_status "info" "Reinstalling with overrides..."
bun install

echo ""
echo "3. Verifying fixes..."
echo "---------------------"

# Check if the vulnerabilities are resolved
if command -v snyk &> /dev/null; then
    print_status "info" "Running security scan to verify fixes..."
    
    # Run a quick test to see if lodash vulnerabilities are still present
    if bun run security:scan 2>&1 | grep -q "lodash@3.10.1"; then
        print_status "warning" "Some lodash vulnerabilities may still be present"
        print_status "info" "Consider adding them to .snyk policy file"
    else
        print_status "success" "Lodash vulnerabilities appear to be resolved"
    fi
else
    print_status "warning" "Snyk not available for verification"
fi

echo ""
echo "4. Additional recommendations..."
echo "--------------------------------"

print_status "info" "To further secure your application:"
echo "   - Regularly run 'bun update' to get latest security patches"
echo "   - Monitor GitHub Dependabot alerts"
echo "   - Run 'bun run security:full' before each release"
echo "   - Review and update .snyk policy file expiration dates"

echo ""
echo "🔧 Vulnerability fixes complete!"