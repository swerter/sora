#!/usr/bin/env bash

echo "Setup garage s3 storage"
export GARAGE_DATA="$PWD/.garage"
export GARAGE_CONF="$GARAGE_DATA/garage.toml"

if [ ! -d "$GARAGE_DATA" ]; then
    echo "Setting up garage"
    mkdir -p "$GARAGE_DATA"

    cat <<EOF > "$GARAGE_CONF"
# Garage config
metadata_dir = "$GARAGE_DATA/meta"
data_dir = "$GARAGE_DATA/data"
db_engine = "lmdb"
metadata_snapshots_dir = "$GARAGE_DATA/snapshots"

replication_factor = 1
consistency_mode = "consistent"

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$(openssl rand -hex 32)"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage.localhost"
index = "index.html"

[k2v_api]
api_bind_addr = "[::]:3904"

[admin]
api_bind_addr = "[::]:3903"
admin_token = "$(openssl rand -base64 32)"
metrics_token = "$(openssl rand -base64 32)"
EOF
fi

cat <<EOF > "./garage_setup.sh"
echo "Setup garage s3 storage"
## export GARAGE_LOG_TO_SYSLOG=1
garage -c "$GARAGE_CONF" server &
node_id=\$(garage -c "$GARAGE_CONF" node id -q | cut -d@ -f1)
garage -c "$GARAGE_CONF" layout assign -z dc1 -c 1G \$node_id
garage -c "$GARAGE_CONF" layout apply
garage -c "$GARAGE_CONF" layout show
garage -c "$GARAGE_CONF" status
garage -c "$GARAGE_CONF" bucket create sorabucket
garage -c "$GARAGE_CONF" bucket list
garage -c "$GARAGE_CONF" key create imap-app-key
garage -c "$GARAGE_CONF" key list
garage -c "$GARAGE_CONF" key info imap-app-key
garage -c "$GARAGE_CONF" bucket allow --read --write --owner sorabucket --key imap-app-key
garage -c "$GARAGE_CONF" repair --yes -a blocks
garage -c "$GARAGE_CONF" stats -a
echo "App key with secret"
garage -c "$GARAGE_CONF" key info imap-app-key --show-secret
EOF

chmod u+x ./garage_setup.sh

alias garage="garage -c $GARAGE_CONF"
echo "Done"
