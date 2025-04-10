{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShells.default = pkgs.mkShell {
          name = "sora-env";
          buildInputs = [
            # goEnv
            # gomod2nix
            pkgs.golangci-lint
            pkgs.go
            pkgs.gotools
            pkgs.go-junit-report
            pkgs.go-task
            pkgs.delve
            pkgs.postgresql
            pkgs.garage
          ];
          env = {
            lang = "en_us.utf-8";

            pgdata = "./.pg";
            pghost = "localhost";
            pgport = "5432";
            pguser = "postgres";
            pgdatabase = "postgres";
            socket_dir = "./.db/postgres-sockets";
          };
          shellhook = ''

            echo "setup postgresql"
            export pgdata="$pwd/.pg"
            export pg_conf="$pgdata/postgresql.conf"
            export socket_dir="$pwd/.db/postgres-sockets"

            if [ ! -d "$pgdata" ]; then
              echo "initializing postgresql..."
              ${pkgs.postgresql}/bin/initdb --auth=trust --username=postgres
              echo "unix_socket_directories = '$socket_dir'" >> "$pgdata/postgresql.conf"
              cat <<eof > "$pg_conf"
              # postgresql configurations
              log_min_messages = warning
              log_min_error_statement = error
              log_min_duration_statement = 100  # ms
              log_connections = on
              log_disconnections = on
              log_duration = on
              log_timezone = 'utc'
              log_statement = 'all'
              log_directory = 'pg_log'
              log_filename = 'postgresql-%y-%m-%d_%h%m%s.log'
              logging_collector = on
              log_min_error_statement = error
            eof
            fi

            function start_postgres() {
            # start postgresql if not running
            if ${pkgs.postgresql}/bin/pg_ctl status -d "$pgdata" >/dev/null; then
              echo "postgresql is already running"
            else
              ${pkgs.postgresql}/bin/pg_ctl -l "$pgdata/postgres.log" start -w
              echo "postgresql started with ${pkgs.postgresql}/bin/pg_ctl"
              # pg_was_started=1
            fi
            }
            function start_postgres() {
            ${pkgs.postgresql}/bin/pg_ctl -d \"$pgdata\" stop 
              echo "postgresql stopped with ${pkgs.postgresql}/bin/pg_ctl"
            }
            alias start_postgres="start_postgres"
            alias stop_postgres="stop_postgres"



            echo "setup garage s3 storage"
            export garage_data="$pwd/.garage"
            export garage_conf="$garage_data/garage.toml"

            if [ ! -d "$garage_data" ]; then
              mkdir -p "$garage_data"

              cat <<eof > "$garage_conf"
              # garage config
              metadata_dir = "$garage_data/meta"
              data_dir = "$garage_data/data"
              db_engine = "lmdb"
              metadata_snapshots_dir = "$garage_data/snapshots"

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
            eof
            fi

            cat <<eof > "./garage_setup.sh"
            echo "setup garage s3 storage"
            ## export garage_log_to_syslog=1
            garage -c "$garage_conf" server
            node_id=$(garage -c "$garage_conf" node id -q | cut -d@ -f1)
            garage -c "$garage_conf" layout assign -z dc1 -c 1g \$node_id
            garage -c "$garage_conf" layout apply
            garage -c "$garage_conf" layout show
            garage -c "$garage_conf" status
            garage -c "$garage_conf" bucket create ex-imap-server-s3
            garage -c "$garage_conf" bucket list
            garage -c "$garage_conf" key create imap-app-key
            garage -c "$garage_conf" key list
            garage -c "$garage_conf" key info imap-app-key
            garage -c "$garage_conf" bucket allow --read --write --owner ex-imap-server-s3 --key imap-app-key
            garage -c "$garage_conf" repair --yes -a blocks
            garage -c "$garage_conf" stats -a
            echo "app key with secret"
            garage -c "$garage_conf" key info imap-app-key --show-secret

            eof
            chmod u+x ./garage_setup.sh

            alias garage="garage -c $garage_conf"
            echo "Setup done"

          '';

        };
      });
}
