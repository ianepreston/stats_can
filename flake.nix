{
  description = "Application packaged using poetry2nix";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable-small";
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # see https://github.com/nix-community/poetry2nix/tree/master#api for more functions and examples.
        pkgs = nixpkgs.legacyPackages.${system};
        inherit (poetry2nix.lib.mkPoetry2Nix { inherit pkgs; })
          mkPoetryApplication mkPoetryEnv;
        pythonVersions = {
          python310 = pkgs.python310;
          python311 = pkgs.python311;
          default = pkgs.python310;
        };
        makePoetryEnvPyVer = pythonPackage:
          mkPoetryEnv {
            projectDir = self;
            editablePackageSources = { my-app = ./src; };
            python = pythonPackage;
            preferWheels = true;
            groups = [ "dev" ];
          };
        makePythonShell = shellName: pythonPackage:
          pkgs.mkShell {
            buildInputs = [ (makePoetryEnvPyVer pythonPackage) ];
          };
        mappedDevShells = builtins.mapAttrs makePythonShell pythonVersions;
        moreDevShells = {
          poetry = pkgs.mkShell { packages = [ pkgs.poetry ]; };
        };
      in { devShells = mappedDevShells // moreDevShells; });
}
