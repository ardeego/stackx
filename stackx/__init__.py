"""stackx - A stackexchange.com dump import library for MySQL.

stackx is a library that is intended to make the import of stackexchange.com
XML dumps into MySQL a breeze.

Current Contents:
    * util -- Miscellaneous useful functions and wrappers
    * archive -- Wrapper around 7z binary used to stream XML dumps into MySQL
    * mysql -- MySQL wrapper

Todo:
    * ...

"""

__author__  = "Niko Rebenich"
__status__  = "beta"
__version__ = "0.0.1"

from .mysql import Connection
from .archive import Archive7z
