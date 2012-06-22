# Makefile for building the Cython file kl_divergence.pyx.
# This builds a shared library kl_divergence.so in the current directory,
# which then can be imported like a normal Python module.

kl_divergence.so: kl_divergence.pyx
	python kl_divergence_setup.py build_ext --inplace
