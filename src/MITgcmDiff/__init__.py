
import os.path as p
import glob

modules = glob.glob(p.join(p.dirname(__file__), "*.py"))
__all__ = [ p.basename(f)[:-3] for f in modules if p.isfile(f) and not f.endswith('__init__.py') ]

#for m in __all__:

#    globals()[m] = __import__("MITgcmDiff.%s" % (m,))

