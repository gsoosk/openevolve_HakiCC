#!/bin/bash

# Run the Java CC Evaluator
# Usage: ./run_evaluator.sh [InitialProgram.java] [output.json] [--pre-known-txn tpcc|auction]
#
# Arguments:
#   InitialProgram.java - Path to the CC implementation file (default: InitialProgram.java)
#   output.json         - Path to output JSON file (default: output.json)
#   --pre-known-txn     - Pre-known transaction type: "tpcc" or "auction" (default: tpcc)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default values
INPUT_FILE=""
OUTPUT_FILE=""
PRE_KNOWN_TXN="tpcc"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pre-known-txn)
            PRE_KNOWN_TXN="$2"
            shift 2
            ;;
        *)
            # Positional arguments: first is input file, second is output file
            if [ -z "$INPUT_FILE" ]; then
                INPUT_FILE="$1"
            elif [ -z "$OUTPUT_FILE" ]; then
                OUTPUT_FILE="$1"
            fi
            shift
            ;;
    esac
done

# Apply defaults for positional arguments
INPUT_FILE="${INPUT_FILE:-InitialProgram.java}"
OUTPUT_FILE="${OUTPUT_FILE:-output.json}"

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
echo "Pre-known transaction type: $PRE_KNOWN_TXN"
java -Xmx120g benchmark.Evaluator --output "$OUTPUT_FILE" --pre-known-txn "$PRE_KNOWN_TXN"

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


