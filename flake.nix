{
  description = "delimiter separated values";
  inputs.nixpkgs.url = "github:nixos/nixpkgs";

  outputs =
    { self, nixpkgs, ... }:
    let
      systems = [
        "aarch64-linux"
        "x86_64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forEachSystem = nixpkgs.lib.genAttrs systems;
      pname = "dsv";
    in
    {
      overlays.default = final: prev: {
        "${pname}-rust" = final.callPackage ./nix/package-rust.nix { };
        "${pname}-python" = final.callPackage ./nix/package-python.nix { };
        "${pname}" = final."${pname}-rust";
      };

      packages = forEachSystem (
        system:
        let
          callPackage = nixpkgs.legacyPackages.${system}.callPackage;
        in
        {
          "${pname}-rust" = callPackage ./nix/package-rust.nix { };
          "${pname}-python" = callPackage ./nix/package-python.nix { };
          "${pname}" = self.packages.${system}."${pname}-rust";
          default = self.packages.${system}.${pname};
        }
      );
    };
}
