from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

# This is a setup file for Cython.  See kl_divergence.pyx.

ext_modules = [Extension("kl_divergence", ["kl_divergence.pyx"])]

setup(
  name = 'Class for probability distribution over words',
  cmdclass = {'build_ext': build_ext},
  ext_modules = ext_modules
)
