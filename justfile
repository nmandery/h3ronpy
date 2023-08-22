# to be executed via https://github.com/casey/just

black:
    black -l 120 python tests *.py docs/source/*.py

ruff:
    ruff check python tests *.py docs/source/*.py

test:
    rm -f dist/*.whl
    maturin build --out dist
    pip install --force-reinstall dist/*.whl
    pytest -s