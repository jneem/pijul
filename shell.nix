with (import <nixpkgs> {});

stdenv.mkDerivation rec {
  name = "rust-pijul-${version}";
  version = "0.3";
  src = ./.;

  buildInputs = [ rustc cargo rustfmt ];
}
