{
  installShellFiles,
  python3Packages,
  shtab ? python3Packages.shtab,
  setuptools ? python3Packages.setuptools,
}:
python3Packages.buildPythonApplication {
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

    mkdir -p $out/bin
    mv ./dsv $out/bin
  '';

  pyproject = true;
  build-system = [ setuptools ];
}
