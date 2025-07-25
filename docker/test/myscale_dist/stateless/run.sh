#!/bin/bash

# fail on errors, verbose and export all env variables
set -e -x -a

# Choose random timezone for this test run.
TZ="$(grep -v '#' /usr/share/zoneinfo/zone.tab | awk '{print $3}' | shuf | head -n1)"
echo "Choosen random timezone $TZ"
ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" >/etc/timezone

# install test configs
/usr/share/clickhouse-test/config/install.sh

# For flaky check we also enable thread fuzzer
if [ "$NUM_TRIES" -gt "1" ]; then
    export THREAD_FUZZER_CPU_TIME_PERIOD_US=1000
    export THREAD_FUZZER_SLEEP_PROBABILITY=0.1
    export THREAD_FUZZER_SLEEP_TIME_US=100000

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY=1

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US=10000

    # simpliest way to forward env variables to server
    clickhouse-server --config /etc/clickhouse-server/config.xml --daemon
else
    sudo clickhouse start
fi

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then

    clickhouse-server --config /etc/clickhouse-server1/config.xml --daemon \
        -- --path /var/lib/clickhouse1/ --logger.stderr /var/log/clickhouse-server/stderr1.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server1.err.log \
        --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
        --mysql_port 19004 --postgresql_port 19005 \
        --keeper_server.tcp_port 19181 --keeper_server.server_id 2 \
        --macros.replica r2 # It doesn't work :(

    clickhouse-server /usr/bin/clickhouse server --config /etc/clickhouse-server2/config.xml --daemon \
        -- --path /var/lib/clickhouse2/ --logger.stderr /var/log/clickhouse-server/stderr2.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server2.err.log \
        --tcp_port 29000 --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 \
        --mysql_port 29004 --postgresql_port 29005 \
        --keeper_server.tcp_port 29181 --keeper_server.server_id 3 \
        --macros.shard s2 # It doesn't work :(

    MAX_RUN_TIME=$((MAX_RUN_TIME < 9000 ? MAX_RUN_TIME : 9000)) # min(MAX_RUN_TIME, 2.5 hours)
    MAX_RUN_TIME=$((MAX_RUN_TIME != 0 ? MAX_RUN_TIME : 9000))   # set to 2.5 hours if 0 (unlimited)
fi

sleep 5

function run_tests() {
    set -x
    # We can have several additional options so we path them as array because it's
    # more idiologically correct.
    read -ra ADDITIONAL_OPTIONS <<<"${ADDITIONAL_OPTIONS:-}"

    # Skip these tests, because they fail when we rerun them multiple times
    if [ "$NUM_TRIES" -gt "1" ]; then
        ADDITIONAL_OPTIONS+=('--order=random')
        ADDITIONAL_OPTIONS+=('--skip')
        ADDITIONAL_OPTIONS+=('00000_no_tests_to_skip')
        # Note that flaky check must be ran in parallel, but for now we run
        # everything in parallel except DatabaseReplicated. See below.
    fi

    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--replicated-database')
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('2')
    else
        # Too many tests fail for DatabaseReplicated in parallel. All other
        # configurations are OK.
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('8')
    fi

    set +e
    clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check --print-time \
        --test-runs "$NUM_TRIES" "${ADDITIONAL_OPTIONS[@]}" 2>&1 |
        ts '%Y-%m-%d %H:%M:%S' |
        tee -a test_output/test_result.txt
    set -e
}

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests || :

python3 ./process_functional_tests_result.py || echo -e "failure\tCannot parse results" >/test_output/check_status.tsv

clickhouse-client -q "system flush logs" || :

grep -Fa "Fatal" /var/log/clickhouse-server/clickhouse-server.log || :
pigz </var/log/clickhouse-server/clickhouse-server.log >/test_output/clickhouse-server.log.gz &
clickhouse-client -q "select * from system.query_log format TSVWithNamesAndTypes" | pigz >/test_output/query-log.tsv.gz &
clickhouse-client -q "select * from system.query_thread_log format TSVWithNamesAndTypes" | pigz >/test_output/query-thread-log.tsv.gz &

# Also export trace log in flamegraph-friendly format.
for trace_type in CPU Memory Real; do
    clickhouse-client -q "
            select
                arrayStringConcat((arrayMap(x -> concat(splitByChar('/', addressToLine(x))[-1], '#', demangle(addressToSymbol(x)) ), trace)), ';') AS stack,
                count(*) AS samples
            from system.trace_log
            where trace_type = '$trace_type'
            group by trace
            order by samples desc
            settings allow_introspection_functions = 1
            format TabSeparated" |
        pigz >"/test_output/trace-log-$trace_type-flamegraph.tsv.gz" &
done

wait || :

mv /var/log/clickhouse-server/stderr.log /test_output/ || :
if [[ -n "$WITH_COVERAGE" ]] && [[ "$WITH_COVERAGE" -eq 1 ]]; then
    tar -chf /test_output/clickhouse_coverage.tar.gz /profraw || :
fi

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination || :

# Replace the engine with Ordinary to avoid extra symlinks stuff in artifacts.
# (so that clickhouse-local --path can read it w/o extra care).
sed -i -e "s/ATTACH DATABASE _ UUID '[^']*'/ATTACH DATABASE system/" -e "s/Atomic/Ordinary/" /var/lib/clickhouse/metadata/system.sql
for table in text_log query_log zookeeper_log trace_log; do
    sed -i "s/ATTACH TABLE _ UUID '[^']*'/ATTACH TABLE $table/" /var/lib/clickhouse/metadata/system/${table}.sql
    tar -chf /test_output/${table}_dump.tar /var/lib/clickhouse/metadata/system.sql /var/lib/clickhouse/metadata/system/${table}.sql /var/lib/clickhouse/data/system/${table} || :
done

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    grep -Fa "Fatal" /var/log/clickhouse-server/clickhouse-server1.log || :
    grep -Fa "Fatal" /var/log/clickhouse-server/clickhouse-server2.log || :
    pigz </var/log/clickhouse-server/clickhouse-server1.log >/test_output/clickhouse-server1.log.gz || :
    pigz </var/log/clickhouse-server/clickhouse-server2.log >/test_output/clickhouse-server2.log.gz || :
    mv /var/log/clickhouse-server/stderr1.log /test_output/ || :
    mv /var/log/clickhouse-server/stderr2.log /test_output/ || :
    tar -chf /test_output/zookeeper_log_dump1.tar /var/lib/clickhouse1/data/system/zookeeper_log || :
    tar -chf /test_output/zookeeper_log_dump2.tar /var/lib/clickhouse2/data/system/zookeeper_log || :
    tar -chf /test_output/coordination1.tar /var/lib/clickhouse1/coordination || :
    tar -chf /test_output/coordination2.tar /var/lib/clickhouse2/coordination || :
fi
