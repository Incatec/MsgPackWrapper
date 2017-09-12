from __future__ import absolute_import, division, print_function, unicode_literals
from setuptools import setup, find_packages
import sys
import os
import shutil

__author__ = 'jleuven'


if len(sys.argv) == 1:  # If no argument provided create wheel file
    sys.argv.append("bdist_wheel")
    sys.argv.append("-d")
    sys.argv.append("/srv/dists")

install_requires = []
if os.path.isfile("requirements.txt"):
    with open("requirements.txt", "r") as req_file:
        install_requires = req_file.readlines()
        for idx, req in enumerate(install_requires):
            install_requires[idx] = str(req.replace("\n", "").replace(" ", ""))


setup(name='MsgPackWrapper',
      version='0.0.2',
      description='Incatec Msgpack wrapper for python',
      url='ssh://git@dev.incatec.nl/diffusion/PLAYERDATA/player_data.git',
      author='jleuven',
      author_email='jleuven@incatec.nl',
      license='INCATEC',
      packages=find_packages(),
      include_package_data=True,
      install_requires=install_requires,
      zip_safe=False)

# run "python setup.py bdist_wheel" to make .whl file (in dist folder)
