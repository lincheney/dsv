{
  installShellFiles,
  rustPlatform,
  autoPatchelfHook,
  python3,
  shtab ? python3.pkgs.shtab,
}:
rustPlatform.buildRustPackage (finalAttrs: {
  pname = "dsv";
  version = "unstable";

  src = ./..;

  nativeBuildInputs = [
    shtab
    installShellFiles
    autoPatchelfHook
  ];

  # force python as lib dependency for autoPatchelf
  # this ensures python is available for `dsv py`
  buildInputs = [ python3 ];
  runtimeDependencies = [ python3 ];

  postInstall = ''
    bash ./make-completions.sh
    installShellCompletion completions/dsv.{bash,zsh}
  '';

  cargoLock.lockFile = ../Cargo.lock;
})
