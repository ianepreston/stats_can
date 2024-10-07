{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    nixpkgs-python.url = "github:cachix/nixpkgs-python";
    nixpkgs-python.inputs = { nixpkgs.follows = "nixpkgs"; };
    devenv.url = "github:cachix/devenv";
  };

  nixConfig = {
    extra-trusted-public-keys =
      "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = { self, nixpkgs, devenv, ... }@inputs:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { system = system; };
      # A list of shell names and their Python versions
      pythonVersions = {
        python39 = "3.9";
        python310 = "3.10";
        python311 = "3.11";
        python312 = "3.12";
        default = "3.10";
      };
      # A function to make a shell with a python version
      makePythonShell = shellName: pythonVersion:
        devenv.lib.mkShell {
          inherit inputs pkgs;
          modules = [
            ({ pkgs, config, ... }: {
              languages.python = {
                version = pythonVersion;
                libraries = with pkgs; [ stdenv.cc.cc.lib gcc-unwrapped libz ];
                enable = true;
                venv.enable = true;
                poetry = {
                  enable = true;
                  activate.enable = true;
                  package = pkgs.poetry;
                  install = {
                    enable = true;
                    installRootPackage = true;
                  };
                };
              };
              env.LD_LIBRARY_PATH =
                "${pkgs.gcc-unwrapped.lib}/lib64:${pkgs.libz}/lib";
            })
          ];
        };
    in {
      # mapAttrs runs the given function (makePythonShell) against every value
      # in the attribute set (pythonVersions) and returns a new set
      devShells.x86_64-linux = builtins.mapAttrs makePythonShell pythonVersions;
    };
}
