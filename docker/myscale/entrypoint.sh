#!/bin/bash

set -eo pipefail
shopt -s nullglob

args=("$@")
arch="$(dpkg --print-architecture)"

CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"

# get CH directories locations
DATA_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=path || true)"
TMP_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=tmp_path || true)"
USER_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=user_files_path || true)"

LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.log || true)"
LOG_DIR=""
if [ -n "$LOG_PATH" ]; then
    LOG_DIR="$(dirname "$LOG_PATH")"
fi

ERROR_LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.errorlog || true)"
ERROR_LOG_DIR=""
if [ -n "$ERROR_LOG_PATH" ]; then
    ERROR_LOG_DIR="$(dirname "$ERROR_LOG_PATH")"
fi

FORMAT_SCHEMA_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=format_schema_path || true)"

CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-}"
CLICKHOUSE_ACCESS_MANAGEMENT="${CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT:-0}"

for dir in "$DATA_DIR" "$ERROR_LOG_DIR" "$LOG_DIR" "$TMP_DIR" "$USER_PATH" "$FORMAT_SCHEMA_PATH"; do
    # check if variable not empty
    [ -z "$dir" ] && continue
    # ensure directories exist
    if ! mkdir -pv "$dir"; then
        echo "Couldn't create necessary directory: $dir"
        exit 1
    fi
done

# if clickhouse user is defined - create it (user "default" already exists out of box)
if [ -n "$CLICKHOUSE_USER" ] && [ "$CLICKHOUSE_USER" != "default" ] || [ -n "$CLICKHOUSE_PASSWORD" ]; then
    echo "$0: create new user '$CLICKHOUSE_USER' instead 'default'"
    cat <<EOT >/etc/clickhouse-server/users.d/default-user.xml
    <clickhouse>
      <!-- Docs: <https://clickhouse.com/docs/en/operations/settings/settings_users/> -->
      <users>
        <!-- Remove default user -->
        <default remove="remove">
        </default>

        <${CLICKHOUSE_USER}>
          <profile>default</profile>
          <networks>
            <ip>::/0</ip>
          </networks>
          <password>${CLICKHOUSE_PASSWORD}</password>
          <quota>default</quota>
          <access_management>${CLICKHOUSE_ACCESS_MANAGEMENT}</access_management>
        </${CLICKHOUSE_USER}>
      </users>
    </clickhouse>
EOT
fi

if [ -n "$(ls /docker-entrypoint-initdb.d/)" ] || [ -n "$CLICKHOUSE_DB" ]; then
    # port is needed to check if clickhouse-server is ready for connections
    HTTP_PORT="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=http_port)"

    # Listen only on localhost until the initialization is done
    /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" -- --listen_host=127.0.0.1 &
    pid="$!"

    # check if clickhouse is ready to accept connections
    # will try to send ping clickhouse via http_port (max 12 retries by default, with 1 sec timeout and 1 sec delay between retries)
    tries=${CLICKHOUSE_INIT_TIMEOUT:-12}
    while ! wget --spider -T 1 -q "http://127.0.0.1:$HTTP_PORT/ping" 2>/dev/null; do
        if [ "$tries" -le "0" ]; then
            echo >&2 'ClickHouse init process failed.'
            exit 1
        fi
        tries=$((tries - 1))
        sleep 1
    done

    clickhouseclient=(clickhouse-client --multiquery --host "127.0.0.1" -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD")

    echo

    # create default database, if defined
    if [ -n "$CLICKHOUSE_DB" ]; then
        echo "$0: create database '$CLICKHOUSE_DB'"
        "${clickhouseclient[@]}" -q "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB"
    fi

    for f in /docker-entrypoint-initdb.d/*; do
        case "$f" in
        *.sh)
            if [ -x "$f" ]; then
                echo "$0: running $f"
                "$f"
            else
                echo "$0: sourcing $f"
                # shellcheck source=/dev/null
                . "$f"
            fi
            ;;
        *.sql)
            echo "$0: running $f"
            "${clickhouseclient[@]}" <"$f"
            echo
            ;;
        *.sql.gz)
            echo "$0: running $f"
            gunzip -c "$f" | "${clickhouseclient[@]}"
            echo
            ;;
        *) echo "$0: ignoring $f" ;;
        esac
        echo
    done

    if ! kill -s TERM "$pid" || ! wait "$pid"; then
        echo >&2 'Finishing of ClickHouse init process failed.'
        exit 1
    fi
fi

# if no args passed to `docker run` or first argument start with `--`, then the user is passing clickhouse-server arguments
if [[ ${#args[@]} -lt 1 ]] || [[ "$args[1]" == "--"* ]]; then
    # Watchdog is launched by default, but does not send SIGINT to the main process,
    # so the container can't be finished by ctrl+c
    CLICKHOUSE_WATCHDOG_ENABLE=${CLICKHOUSE_WATCHDOG_ENABLE:-0}
    export CLICKHOUSE_WATCHDOG_ENABLE
    exec /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" "${args[@]}"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "${args[@]}"
