{
  description = "Sluice development environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
          config = {
            allowUnfree = true;
          };
        };

        # rust versions
        rustToolchain = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
          extensions = [ "rust-src" ];
        };

        rustNativeDeps = with pkgs; [
          pkg-config
          sqlite
        ];

        devDependencies = with pkgs; [
          curl
          jq
          pnpm
        ];

        # if a shell is spawned, grab the right shell for the user and wrap it
        shellWrapper = pkgs.writeShellScript "nix-develop-shell" ''
          exec "''${SHELL}" -i
        '';
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs =
            devDependencies
            ++ rustNativeDeps
            ++ [
              rustToolchain
            ];

          shell = shellWrapper;

          RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";

          shellHook = ''
            echo "dev shell: rust version: $(rustc --version)"
          '';
        };
      }
    );
}
