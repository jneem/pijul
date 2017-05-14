with (import <nixpkgs> {});

with rustPlatform;

buildRustPackage rec {
  name = "pijul-${version}";
  version = "0.3.3";

  src = ./.;

  sourceRoot = "pijul/pijul";

  buildInputs = [ perl ]++ stdenv.lib.optionals stdenv.isDarwin
    (with darwin.apple_sdk.frameworks; [ Security ]);

  doCheck = false;
  
  depsSha256 = null;

  meta = with stdenv.lib; {
    description = "A distributed version control system";
    homepage = https://pijul.org;
    license = with licenses; [ gpl2Plus ];
    maintainers = [ maintainers.gal_bolle ];
    platforms = platforms.all;
  };
}
