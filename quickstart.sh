#!/bin/bash
# Quick start script for Flink Notebooks

set -e

echo "=========================================="
echo "Flink Notebooks - Quick Start"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."

# Check Java
if ! command -v java &> /dev/null; then
    echo "❌ Java not found. Please install Java 11 or later."
    exit 1
fi

java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
if [ "$java_version" -lt 11 ]; then
    echo "❌ Java 11+ required. Found version: $java_version"
    exit 1
fi
echo "✅ Java $java_version"

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Please install Node.js 18 or later."
    exit 1
fi
echo "✅ Node.js"

echo ""
echo "All prerequisites met!"
echo ""

# Step 1: Build Flink Runtime
echo "=========================================="
echo "Step 1/2: Building Flink Runtime"
echo "=========================================="
cd flink-runtime

if [ ! -f "build/libs/flink-minicluster.jar" ]; then
    echo "Building Java application (this may take a few minutes)..."
    ./gradlew build --quiet
    echo "✅ Flink runtime built successfully"
else
    echo "✅ Flink runtime already built (skipping)"
fi

cd ..
echo ""

# Step 2: Build VSCode Extension
echo "=========================================="
echo "Step 2/2: Building VSCode Extension"
echo "=========================================="
cd vscode-extension

if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm install --silent
    echo "✅ npm dependencies installed"
fi

echo "Compiling TypeScript..."
npm run compile --silent
echo "✅ Extension compiled"

cd ..
echo ""
