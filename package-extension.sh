#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}   Flink Notebooks Extension Packager${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Get script directory (project root)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_RUNTIME_DIR="$PROJECT_ROOT/flink-runtime"
VSCODE_EXTENSION_DIR="$PROJECT_ROOT/vscode-extension"
EXTENSION_RUNTIME_LINK="$VSCODE_EXTENSION_DIR/flink-runtime"

# Step 1: Check prerequisites
echo -e "${YELLOW}[1/6] Checking prerequisites...${NC}"

# Check Java
if ! command -v java &> /dev/null; then
    echo -e "${RED}❌ Java not found. Please install Java 17+ and try again.${NC}"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 17 ]; then
    echo -e "${RED}❌ Java version $JAVA_VERSION found, but Java 17+ is required.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Java $JAVA_VERSION detected${NC}"

# Check Node.js
if ! command -v node &> /dev/null; then
    echo -e "${RED}❌ Node.js not found. Please install Node.js 18+ and try again.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Node.js $(node --version) detected${NC}"

# Check npm
if ! command -v npm &> /dev/null; then
    echo -e "${RED}❌ npm not found. Please install npm and try again.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ npm $(npm --version) detected${NC}"

# Add node_modules/.bin to PATH if vsce exists locally
if [ -f "$PROJECT_ROOT/node_modules/.bin/vsce" ]; then
    export PATH="$PROJECT_ROOT/node_modules/.bin:$PATH"
fi

# Check vsce (VSCode Extension Manager)
if ! command -v vsce &> /dev/null; then
    echo -e "${RED}❌ vsce not found.${NC}"
    echo -e "${YELLOW}   Please run: ${BLUE}direnv allow${NC} ${YELLOW}to load Nix flake environment${NC}"
    echo -e "${YELLOW}   Or install manually: ${BLUE}npm install -g @vscode/vsce${NC}"
    exit 1
fi
echo -e "${GREEN}✓ vsce $(vsce --version) detected${NC}"

echo ""

# Step 2: Build Flink Runtime JAR
echo -e "${YELLOW}[2/6] Building Flink Runtime JAR...${NC}"
cd "$FLINK_RUNTIME_DIR"

if [ ! -f "gradlew" ]; then
    echo -e "${RED}❌ gradlew not found in $FLINK_RUNTIME_DIR${NC}"
    exit 1
fi

# Build main runtime JAR
echo "Building flink-minicluster.jar..."
./gradlew clean shadowJar --quiet || ./gradlew clean shadowJar

# Check if JAR was created
JAR_PATH="$FLINK_RUNTIME_DIR/build/libs/flink-minicluster.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo -e "${RED}❌ Failed to build flink-minicluster.jar${NC}"
    exit 1
fi

JAR_SIZE=$(du -h "$JAR_PATH" | cut -f1)
echo -e "${GREEN}✓ Built flink-minicluster.jar ($JAR_SIZE)${NC}"

# Build UDF subproject (creates JAR with bundled examples)
echo "Building flink-udfs.jar..."
./gradlew :udfs:shadowJar --quiet || ./gradlew :udfs:shadowJar

# Check if UDF JAR was created
UDF_JAR_PATH="$FLINK_RUNTIME_DIR/lib/flink-udfs.jar"
if [ -f "$UDF_JAR_PATH" ]; then
    UDF_JAR_SIZE=$(du -h "$UDF_JAR_PATH" | cut -f1)
    echo -e "${GREEN}✓ Built flink-udfs.jar ($UDF_JAR_SIZE)${NC}"
else
    echo -e "${YELLOW}⚠ UDF JAR not found (this is OK)${NC}"
fi

echo ""

# Step 3: Copy flink-runtime into extension directory
echo -e "${YELLOW}[3/6] Copying Flink runtime to extension directory...${NC}"

# Remove existing directory if it exists
if [ -d "$EXTENSION_RUNTIME_LINK" ]; then
    rm -rf "$EXTENSION_RUNTIME_LINK"
    echo "Removed existing directory"
fi

# Create the directory
mkdir -p "$EXTENSION_RUNTIME_LINK"

# Copy only the necessary files (selective copy to reduce package size)
echo "Copying JAR files..."
mkdir -p "$EXTENSION_RUNTIME_LINK/build/libs"
cp "$FLINK_RUNTIME_DIR/build/libs/flink-minicluster.jar" "$EXTENSION_RUNTIME_LINK/build/libs/" 2>/dev/null || echo "Note: Main JAR will be copied if it exists"

echo "Copying UDF JAR and lib directory..."
mkdir -p "$EXTENSION_RUNTIME_LINK/lib"
if [ -f "$FLINK_RUNTIME_DIR/lib/flink-udfs.jar" ]; then
    cp "$FLINK_RUNTIME_DIR/lib/flink-udfs.jar" "$EXTENSION_RUNTIME_LINK/lib/"
fi

echo "Copying configuration..."
cp -r "$FLINK_RUNTIME_DIR/conf" "$EXTENSION_RUNTIME_LINK/"

echo "Copying Gradle wrapper..."
cp "$FLINK_RUNTIME_DIR/gradlew" "$EXTENSION_RUNTIME_LINK/"
cp "$FLINK_RUNTIME_DIR/gradlew.bat" "$EXTENSION_RUNTIME_LINK/" 2>/dev/null || true
cp -r "$FLINK_RUNTIME_DIR/gradle" "$EXTENSION_RUNTIME_LINK/"

echo "Copying build files..."
cp "$FLINK_RUNTIME_DIR/build.gradle" "$EXTENSION_RUNTIME_LINK/"
cp "$FLINK_RUNTIME_DIR/settings.gradle" "$EXTENSION_RUNTIME_LINK/"
cp "$FLINK_RUNTIME_DIR/gradle.properties" "$EXTENSION_RUNTIME_LINK/"

echo "Copying UDF subproject..."
cp -r "$FLINK_RUNTIME_DIR/udfs" "$EXTENSION_RUNTIME_LINK/"
# Clean UDF build artifacts
rm -rf "$EXTENSION_RUNTIME_LINK/udfs/build"

echo -e "${GREEN}✓ Copied flink-runtime directory${NC}"

echo ""

# Step 4: Install Node dependencies
echo -e "${YELLOW}[4/6] Installing Node dependencies...${NC}"
cd "$VSCODE_EXTENSION_DIR"

npm install --silent || npm install
echo -e "${GREEN}✓ Dependencies installed${NC}"

echo ""

# Step 5: Compile TypeScript
echo -e "${YELLOW}[5/6] Compiling TypeScript...${NC}"
cd "$VSCODE_EXTENSION_DIR"

npm run compile

if [ ! -d "out" ]; then
    echo -e "${RED}❌ TypeScript compilation failed - 'out' directory not found${NC}"
    exit 1
fi

echo -e "${GREEN}✓ TypeScript compiled to out/${NC}"

echo ""

# Step 6: Package extension
echo -e "${YELLOW}[6/6] Packaging VSCode extension...${NC}"
cd "$VSCODE_EXTENSION_DIR"

# Get version from package.json
VERSION=$(node -p "require('./package.json').version")
VSIX_NAME="flink-notebooks-${VERSION}.vsix"

# Remove old VSIX if exists
if [ -f "$VSIX_NAME" ]; then
    rm "$VSIX_NAME"
    echo "Removed old $VSIX_NAME"
fi

# Package the extension (skip secret scanning to avoid directory errors)
echo "Running vsce package..."
vsce package --no-git-tag-version --skip-license

# Clean up copied directory after packaging
if [ -d "$EXTENSION_RUNTIME_LINK" ]; then
    rm -rf "$EXTENSION_RUNTIME_LINK"
    echo "Cleaned up flink-runtime directory"
fi

if [ ! -f "$VSIX_NAME" ]; then
    echo -e "${RED}❌ Failed to create VSIX package${NC}"
    exit 1
fi

VSIX_SIZE=$(du -h "$VSIX_NAME" | cut -f1)
VSIX_PATH="$VSCODE_EXTENSION_DIR/$VSIX_NAME"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}   ✓ Extension packaged successfully!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo -e "📦 Package: ${BLUE}$VSIX_NAME${NC} (${VSIX_SIZE})"
echo -e "📍 Location: ${BLUE}$VSIX_PATH${NC}"
echo ""
echo -e "${YELLOW}To install locally:${NC}"
echo -e "  1. Open VSCode"
echo -e "  2. Run: ${BLUE}Extensions: Install from VSIX...${NC}"
echo -e "  3. Select: ${BLUE}$VSIX_PATH${NC}"
echo ""
echo -e "${YELLOW}Or install via command line:${NC}"
echo -e "  ${BLUE}code --install-extension \"$VSIX_PATH\"${NC}"
echo ""
echo -e "${YELLOW}To uninstall previous version:${NC}"
echo -e "  ${BLUE}code --uninstall-extension flink-notebooks.flink-notebooks${NC}"
echo ""
echo -e "${GREEN}Next steps:${NC}"
echo -e "  1. Install the extension"
echo -e "  2. Run: ${BLUE}Flink: New Flink Notebook${NC}"
echo -e "  3. Write SQL and execute with ${BLUE}Shift+Enter${NC}"
echo ""
