# install the dependencies needed for development and ci by
# collecting them from all relevant files

import subprocess
from pathlib import Path

import sys


def pip_install(packages):
    if packages:
        subprocess.run(["pip", "install", "--upgrade"] + packages, stdout=sys.stdout, stderr=sys.stderr)


if __name__ == '__main__':
    pip_install(["pip", "toml"])  # always upgrade pip

    import toml  # import only after it has been installed

    directory = Path(__file__).parent
    packages = []

    pyproject_toml = toml.load(directory / "pyproject.toml")
    for pkg in pyproject_toml.get("build-system", {}).get("requires"):
        packages.append(pkg)

    pytest = pyproject_toml.get("tool", {}).get("pytest")
    if pytest is not None:
        pytest_package = "pytest"
        pytest_minversion = pytest.get("ini_options", {}).get("minversion")
        if pytest_minversion:
            packages.append(f"{pytest_package}>={pytest_minversion}")
        else:
            packages.append(f"{pytest_package}")

    for pkg in toml.load(directory / "Cargo.toml").get("package", {}).get("metadata", {}).get("maturin", {}).get(
            "requires-dist", []):
        packages.append(pkg)
    pip_install(packages)
