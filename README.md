# py-sentenai
[![Build Status](https://travis-ci.org/Sentenai/py-sentenai.svg?branch=master)](https://travis-ci.org/Sentenai/py-sentenai)

Python client library for sentenai.

### Development

It's recommended you use pyenv and pyenv-virtualenv to manage your
python versions and locally installed pip dependencies. pyenv-virtualenv
also allows you to manage your anaconda installations. This repository
should be compatible with python 2.7+ and 3.5+, however development
happens in python-3.6.0.

This library depends [shapely][], which in turn depends on the static
Geos C library:
  - ubuntu: `sudo apt install libgeos-c1v5`
  - freebsd: `sudo pkg install geos`
  - archlinux: `sudo pacman -S geos`
  - MacOS: `brew install geos`

[shapely]:https://github.com/Toblerity/Shapely
