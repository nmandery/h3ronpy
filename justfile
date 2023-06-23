# to be executed via https://github.com/casey/just

black:
    black -l 120 h3ronpy tests *.py docs/source/*.py

ruff:
    ruff check h3ronpy tests *.py docs/source/*.py