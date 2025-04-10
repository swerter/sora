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
            pkgs.go_1_21
            pkgs.gotools
            pkgs.go-junit-report
            pkgs.go-task
            pkgs.delve
            pkgs.postgresql
            pkgs.garage
          ];
        };
      });
}
