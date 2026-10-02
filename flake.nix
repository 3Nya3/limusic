# Community-maintained development shell; it is not covered by CI.
{
  description = "Development environment for Limusic";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          linuxPackages = with pkgs; [
            gtk3
            libayatana-appindicator
            dbus
            mpv
            librsvg
            openssl
            pkg-config
            glib-networking
            webkitgtk_4_1
          ];
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              nodejs
              pnpm
              rustc
              rustfmt
              cargo-tauri
            ] ++ linuxPackages;

            shellHook = ''
              export RUST_SRC_PATH="${pkgs.rustPlatform.rustLibSrc}"
              export GIO_MODULE_DIR="${pkgs.glib-networking}/lib/gio/modules"
              export GIO_EXTRA_MODULES="${pkgs.glib-networking}/lib/gio/modules"
              # GTK's file chooser (the local-music folder picker) aborts without its GSettings schemas.
              export XDG_DATA_DIRS="$GSETTINGS_SCHEMAS_PATH:$XDG_DATA_DIRS"
              export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath linuxPackages}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            '';
          };
        });
    };
}
