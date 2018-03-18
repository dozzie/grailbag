#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name = "grailbag",
    version = "0.0.0",
    description = "GrailBag client",
    packages    = find_packages("lib"),
    scripts      = ["bin/grailbag"],
    package_dir = { "": "lib" },
    install_requires = [],
)
