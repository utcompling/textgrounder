# Makefile for building the Cython file word_distribution.pyx.
# This builds a shared library word_distribution.so in the current directory,
# which then can be imported like a normal Python module.

word_distribution.so: word_distribution.pyx
	python word_distribution_setup.py build_ext --inplace
