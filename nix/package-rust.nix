{
  installShellFiles,
  rustPlatform,
  python3Packages,
  shtab ? python3Packages.shtab,
}:
rustPlatform.buildRustPackage (finalAttrs: {
  pname = "dsv";
  version = "unstable";

  src = ./..;

  nativeBuildInputs = [
    shtab
    installShellFiles
  ];

  postInstall = ''
    bash ./make-completions.sh
    installShellCompletion completions/dsv.{bash,zsh}
  '';

  cargoLock.lockFile = ../Cargo.lock;
})
