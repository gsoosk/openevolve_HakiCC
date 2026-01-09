#!/bin/bash

# Run the Java CC Evaluator
# Usage: ./run_evaluator.sh [InitialProgram.java] [output.json]
#
# Arguments:
#   InitialProgram.java - Path to the CC implementation file (default: InitialProgram.java)
#   output.json         - Path to output JSON file (default: output.json)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse arguments
INPUT_FILE="${1:-InitialProgram.java}"
OUTPUT_FILE="${2:-output.json}"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found"
    exit 1
fi

# Use a temporary filename for the implementation source code
TEMP_CC_FILE="CCImplementationTemp.java"

# If input file is not 'CCImplementationTemp.java', copy it for compilation
INPUT_BASENAME=$(basename "$INPUT_FILE")
if [ "$INPUT_BASENAME" != "$TEMP_CC_FILE" ]; then
    cp "$INPUT_FILE" "$TEMP_CC_FILE"
else
    # Already the correct temp file
    :
fi

echo "Compiling Java files..."
javac -d . KVStore.java Concurrency.java "$TEMP_CC_FILE" Evaluator.java

if [ $? -ne 0 ]; then
    echo "Error: Compilation failed"
    rm -f "$TEMP_CC_FILE"
    exit 1
fi

echo "Running evaluator..."
java -Xmx120g benchmark.Evaluator --output "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo ""
    echo "Results:"
    cat "$OUTPUT_FILE"
else
    echo "Error: Evaluation failed"
    rm -f "$TEMP_CC_FILE"
    exit 1
fi

# Clean up the temporary copy if it was created here
if [ "$INPUT_BASENAME" != "$TEMP_CC_FILE" ]; then
    rm -f "$TEMP_CC_FILE"
fi


