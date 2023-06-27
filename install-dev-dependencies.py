# install the dependencies needed for development and ci by
# collecting them from all relevant files

import subprocess
from pathlib import Path

import sys
import os


def install(packages, upgrade=False):
    pkg_manager = os.environ.get("PKG_MANAGER") or "pip"
    cmd = [pkg_manager, "install"]
    if upgrade and pkg_manager == "pip":
        cmd.append("--upgrade")
    if packages:
        subprocess.run(cmd + packages, stdout=sys.stdout, stderr=sys.stderr)


if __name__ == "__main__":
    install(["pip", "toml", "black", "ruff"], upgrade=True)  # always upgrade pip

    packages = []

    if sys.platform == "linux":
        packages.append("patchelf")

    def harvest_deps(section, keys):
        for k in keys:
            packages.extend(section.get(k, []))

    import toml  # import only after it has been installed

    pyproject_toml = toml.load(Path(__file__).parent / "pyproject.toml")
    harvest_deps(pyproject_toml.get("build-system", {}), ("requires", "requires-dist"))
    project = pyproject_toml.get("project", {})
    harvest_deps(project, ("dependencies",))

    for deps in project.get("optional-dependencies", {}).values():
        packages.extend(deps)

    pytest = pyproject_toml.get("tool", {}).get("pytest")
    if pytest is not None:
        pytest_package = "pytest"
        pytest_minversion = pytest.get("ini_options", {}).get("minversion")
        if pytest_minversion:
            packages.append(f"{pytest_package}>={pytest_minversion}")
        else:
            packages.append(f"{pytest_package}")

    install(packages)
