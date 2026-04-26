{
  description = "Silver + Gold dbt models and embed-enrich service for Project Foundry";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.flake-parts.url = "github:hercules-ci/flake-parts";
  inputs.statement-extract = {
    url = "github:lorcan17/statement-extract";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = inputs@{ flake-parts, statement-extract, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-darwin" ];

      perSystem = { pkgs, system, ... }: let
        python = pkgs.python312;
        statementExtractPkg = statement-extract.packages.${system}.default;

        # Inline dbt-duckdb derivation — missing from nixpkgs as of 2026-04
        # (tracked in PR #457151). Inlined here rather than via overlay to
        # avoid invalidating the binary cache for the whole python312 set.
        dbt-duckdb = python.pkgs.buildPythonPackage rec {
          pname = "dbt-duckdb";
          version = "1.10.1";
          pyproject = true;
          src = pkgs.fetchFromGitHub {
            owner = "duckdb";
            repo = "dbt-duckdb";
            tag = version;
            hash = "sha256-Xqd2u2x0rPfPwFYNDJPvQzCNyDa9TpmdSWQLyKRMLtk=";
          };
          build-system = with python.pkgs; [ setuptools pbr ];
          dependencies = with python.pkgs; [ dbt-common dbt-adapters dbt-core duckdb ];
          env.PBR_VERSION = version;
          pythonImportsCheck = [ "dbt.adapters.duckdb" ];
          doCheck = false;
        };

        # Combined Python env: embed_enrich runtime deps + dbt-duckdb + the
        # statement-extract package (consumed as a library, not just a CLI).
        pythonEnv = python.withPackages (ps: with ps; [
          dbt-core
          duckdb
          openai
          httpx
          pydantic
        ] ++ [ dbt-duckdb statementExtractPkg ]);

        # Ship the dbt project tree (models, seeds, profiles.yml, dbt_project.yml,
        # the embed_enrich Python module) under $out/share so wrappers can cd into
        # it. Real seeds (gitignored *.csv) are populated at runtime by the
        # OptiPlex systemd unit before `dbt seed` runs.
        projectTree = pkgs.stdenv.mkDerivation {
          pname = "finance-lake-tree";
          version = "0.1.0";
          src = ./.;
          installPhase = ''
            mkdir -p $out/share/finance-lake
            cp -r dbt_project.yml profiles.yml models seeds embed_enrich ingest \
              $out/share/finance-lake/
          '';
        };

      in {
        packages = {
          default = pkgs.symlinkJoin {
            name = "finance-lake";
            paths = [
              projectTree
              (pkgs.writeShellApplication {
                name = "embed-enrich";
                runtimeInputs = [ pythonEnv ];
                text = ''
                  cd ${projectTree}/share/finance-lake
                  exec python -m embed_enrich "$@"
                '';
              })
              (pkgs.writeShellApplication {
                name = "finance-lake-dbt";
                runtimeInputs = [ pythonEnv ];
                text = ''
                  cd ${projectTree}/share/finance-lake
                  exec dbt "$@"
                '';
              })
              (pkgs.writeShellApplication {
                name = "ingest-paperless-hook";
                runtimeInputs = [ pythonEnv ];
                text = ''
                  cd ${projectTree}/share/finance-lake
                  exec python -m ingest.adapters.paperless "$@"
                '';
              })
            ];
            meta.description = "Project Foundry: embed-enrich + dbt orchestration";
          };

          # Expose the python env separately so the systemd unit can also run
          # ad-hoc python scripts (scripts/dev_bootstrap.py etc.) if needed.
          inherit pythonEnv;
        };

        devShells.default = pkgs.mkShell {
          packages = [ pkgs.uv python ];
        };
      };
    };
}
