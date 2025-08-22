{
  description = "Stats Can flake using uv2nix";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      uv2nix,
      pyproject-nix,
      pyproject-build-systems,
      ...
    }:
    let
      inherit (nixpkgs) lib;

      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

      overlay = workspace.mkPyprojectOverlay {
        sourcePreference = "wheel";
      };

      pyprojectOverrides = _final: _prev: {
        # Implement build fixups here.
      };

      pkgs = nixpkgs.legacyPackages.x86_64-linux;

      pythonVersions = {
        python310 = pkgs.python310;
        python311 = pkgs.python311;
        python312 = pkgs.python312;
        python313 = pkgs.python313;
        default = pkgs.python313; # This will create a 'default' shell
      };

      # --- CHANGED: A function to generate a dev shell for a given Python version ---
      # This function encapsulates all the logic that was previously hardcoded for a single Python version.
      mkPythonDevShell =
        python:
        let
          # 'pythonSet' is now defined inside this function, using the 'python' argument.
          pythonSet =
            (pkgs.callPackage pyproject-nix.build.packages {
              inherit python;
            }).overrideScope
              (
                lib.composeManyExtensions [
                  pyproject-build-systems.overlays.default
                  overlay
                  pyprojectOverrides
                ]
              );

          # The rest of the shell-building logic is also moved inside.
          editableOverlay = workspace.mkEditablePyprojectOverlay {
            root = "$REPO_ROOT";
          };

          editablePythonSet = pythonSet.overrideScope (
            lib.composeManyExtensions [
              editableOverlay
              (final: prev: {
                stats-can = prev.stats-can.overrideAttrs (old: {
                  src = lib.fileset.toSource {
                    root = old.src;
                    fileset = lib.fileset.unions [
                      (old.src + "/pyproject.toml")
                      (old.src + "/README.md")
                      (old.src + "/src/stats_can/__init__.py")
                    ];
                  };
                  nativeBuildInputs =
                    old.nativeBuildInputs
                    ++ final.resolveBuildSystem {
                      editables = [ ];
                    };
                });
              })
            ]
          );

          virtualenv = editablePythonSet.mkVirtualEnv "stats-can-dev-env" workspace.deps.all;

        in
        pkgs.mkShell {
          packages = [
            virtualenv
            pkgs.uv
          ];

          env = {
            UV_NO_SYNC = "1";
            # Use the 'python' argument to set the interpreter path.
            UV_PYTHON = python.interpreter;
            UV_PYTHON_DOWNLOADS = "never";
          };

          shellHook = ''
            unset PYTHONPATH
            export REPO_ROOT=$(git rev-parse --show-toplevel)
          '';
        };

    in
    {
      # --- CHANGED: Generate multiple dev shells using lib.mapAttrs ---
      # We map over the 'pythonVersions' set. For each entry, we call our 'mkPythonDevShell' function.
      # The key of the entry (e.g., "python312") becomes the name of the shell.
      devShells.x86_64-linux = lib.mapAttrs (_name: pythonPkg: mkPythonDevShell pythonPkg) pythonVersions;
    };
}
