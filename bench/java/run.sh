#!/bin/sh
# Builds and runs the JMH suite, then writes JSON for `go run ./cmd/report -java`.
# Usage: run.sh [-o report.json] [jmh args...]   (default output: target/report.json)
set -eu

out=target/report.json
if [ "${1:-}" = "-o" ]; then
	case $2 in
	/*) out=$2 ;;
	*) out=$PWD/$2 ;;
	esac
	shift 2
fi

cd "$(dirname "$0")"
BENCH_DATASETS=$(realpath ../datasets)
export BENCH_DATASETS

mvn -q -B -DskipTests package

java -jar target/benchmarks.jar -rf json -rff target/jmh.json -prof gc "$@"
java -cp target/benchmarks.jar io.github.iskorotkov.avrobench.jmh.Convert target/jmh.json "$out"
echo "report json: $out" >&2
