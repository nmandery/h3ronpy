# to be executed via https://github.com/casey/just

readme:
    jupyter nbconvert --to Markdown --execute README.ipynb
    git add README_files/*
    git add README.md
