#!/bin/bash

# Security check script for synthetics-monitor-extractor
# This script checks for common security issues and provides recommendations

set -euo pipefail

echo "🔒 Running Security Check for synthetics-monitor-extractor"
echo "========================================================="
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

echo "1. Checking Docker base image security..."
echo "-----------------------------------------"

# Check if we're using a stable version
if grep -q "oven/bun:1.1.42-alpine" Dockerfile; then
    print_status "success" "Using stable Bun version (1.1.42-alpine)"
else
    print_status "warning" "Consider using oven/bun:1.1.42-alpine for better security"
fi

# Check if we're updating Alpine packages
if grep -q "apk update" Dockerfile && grep -q "apk upgrade" Dockerfile; then
    print_status "success" "Alpine packages are being updated"
else
    print_status "warning" "Add 'apk update && apk upgrade' to Dockerfile"
fi

echo ""
echo "2. Checking for security best practices..."
echo "-----------------------------------------"

# Check if running as non-root user
if grep -q "USER bun" Dockerfile; then
    print_status "success" "Container runs as non-root user (bun)"
else
    print_status "error" "Container should run as non-root user"
fi

# Check for exposed secrets in environment
if grep -q "PASSWORD=\"\"" Dockerfile && grep -q "USERNAME=\"\"" Dockerfile; then
    print_status "success" "No hardcoded credentials in Dockerfile"
else
    print_status "warning" "Ensure no credentials are hardcoded"
fi

echo ""
echo "3. Checking dependency security..."
echo "-----------------------------------"

# Check if .snyk file exists
if [ -f ".snyk" ]; then
    print_status "success" "Snyk policy file exists"
else
    print_status "warning" "No .snyk policy file found"
fi

# Check package.json for security scripts
if grep -q "security:scan" package.json; then
    print_status "success" "Security scanning scripts are configured"
else
    print_status "warning" "Add security scanning scripts to package.json"
fi

echo ""
echo "4. Checking application security features..."
echo "--------------------------------------------"

# Check for input validation
if grep -q "zod" package.json; then
    print_status "success" "Using Zod for input validation"
else
    print_status "warning" "Consider using a validation library"
fi

# Check for rate limiting
if grep -q "rateLimit" src/config.ts 2>/dev/null; then
    print_status "success" "Rate limiting is configured"
else
    print_status "warning" "Consider implementing rate limiting"
fi

# Check for circuit breaker pattern
if [ -f "src/utils/circuitBreaker.ts" ]; then
    print_status "success" "Circuit breaker pattern implemented"
else
    print_status "warning" "Consider implementing circuit breaker pattern"
fi

echo ""
echo "5. Security Recommendations..."
echo "-------------------------------"

print_status "info" "Run regular security scans:"
echo "   - bun run security:scan (dependencies)"
echo "   - bun run security:code (code analysis)"
echo "   - bun run docker:security (container scan)"

print_status "info" "Update dependencies regularly:"
echo "   - bun update (update all dependencies)"
echo "   - docker pull oven/bun:1.1.42-alpine (update base image)"

print_status "info" "Monitor for new vulnerabilities:"
echo "   - Set up GitHub Dependabot alerts"
echo "   - Configure Snyk monitoring"
echo "   - Review security advisories regularly"

echo ""
echo "🔒 Security check complete!"
echo ""

# Run actual security scan if available
if command -v snyk &> /dev/null; then
    echo "Running Snyk security scan..."
    echo "-----------------------------"
    bun run security:scan || print_status "warning" "Some vulnerabilities found - review above"
else
    print_status "info" "Install Snyk CLI for detailed security scanning"
fi