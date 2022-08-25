# install the dependencies needed for development and ci by
# collecting them from all relevant files

import subprocess
from pathlib import Path

import sys
import os

def install(packages, upgrade=False):
    pkg_manager = os.environ.get("PKG_MANAGER") or 'pip'
    cmd = [pkg_manager, "install"]
    if upgrade and pkg_manager == 'pip':
        cmd.append("--upgrade")
    if packages:
        subprocess.run(cmd + packages, stdout=sys.stdout, stderr=sys.stderr)


if __name__ == '__main__':
    install(["pip", "toml"], upgrade=True)  # always upgrade pip

    import toml  # import only after it has been installed

    directory = Path(__file__).parent
    packages = []

    pyproject_toml = toml.load(directory / "pyproject.toml")
    pyproject_bs = pyproject_toml.get("build-system", {})
    for k in ("requires", "requires-dist"):
        for pkg in pyproject_bs.get(k, []):
            packages.append(pkg)

    pytest = pyproject_toml.get("tool", {}).get("pytest")
    if pytest is not None:
        pytest_package = "pytest"
        pytest_minversion = pytest.get("ini_options", {}).get("minversion")
        if pytest_minversion:
            packages.append(f"{pytest_package}>={pytest_minversion}")
        else:
            packages.append(f"{pytest_package}")

    install(packages)
