"""archive.py - Wrapper around 7z binary for extraction of individual files.

Note:
    Every call to ``extract()`` must be followed by a call to ``join()``

"""
import os
import re
import subprocess
import tempfile
import threading
from .utils import which, is_exe

DEFAULT_BIN = "7z"
"""str: Default binary name for 7z"""

class Archive7z(object):
    """Wrapper around 7z binary for extraction of individual files.

    Wrapper around 7z binary for extraction of individual files either
    to disk or to a named pipe.

    Note:
        Every call to ``extract()`` must be followed by a call to ``join()``

    """

    outpath = ""
    _xfilename = ""
    _outfile = None
    _thread = None
    _pipe = False
    _subprocess = None

    def __init__(self, filename, cmd="", config=None):
        """Construct wrapper around 7z binary.

        Note:
            ``cmd`` is an option parameter and when not specified
             the system path will be searched for the 7z binary

        Args:
            filename (str): filename of 7z archive
            cmd (str): path ot 7z binary
            config (dict): configuration via json dict

        Attributes:
            outpath (str): path to extracted file/named pipe

        """
        self.filename = filename
        if config:
            command = config["cmd"];
        else:
            command = cmd
        if not command:
            command = which(DEFAULT_BIN)
        if not is_exe(command):
            raise OSError("Could not find 7z binary")
        self.cmd = command

    def _make_pipe(self, fifopath):
        self.outpath = tempfile.mktemp(suffix=".fifo", dir=fifopath)
        try:
            os.mkfifo(self.outpath)
        except OSError as e:
            raise OSError("Failed to create FIFO: {}".format(e))
        else:
            return self.outpath

    def _extract(self):
        self._outfile = open(self.outpath, "w")
        cmd = [self.cmd, "e", "-so", self.filename, self._xfilename]
        self._subprocess = subprocess.Popen(cmd, stdout=self._outfile)
        self._subprocess.communicate()
        self._outfile.close()
        if self._pipe:
            os.remove(self.outpath)
        self._outfile = None

    def list_files(self, ext=None):
        """Return list of files contained in 7z archive.

        Args:
            ext (str): file extension filter (e.g. ".xml")

        Returns:
            list: list of files contained in 7z archive

        Todo:
            handle directories properly

        """
        cmd = [self.cmd, "l", self.filename]
        self._subprocess = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = self._subprocess.communicate()
        list_ = [m[2]  for m in re.findall(r"^\d{4}(-\d{2}){2}.*(\d\s+)(.*)$", out.decode('utf8'), re.M)]
        list_ = list_[:-1]
        if ext:
            if not ext[0] == ".":
                ext = "." + ext
            list_ = [filename for filename in list_ if os.path.splitext(filename)[1] == ext]
        return list_

    def extract(self, xfilename, outdir, pipe=False):
        """Extract file from 7z achieve to file system or named pipe.

        Note:
            Every call to ``extract()`` must be followed by a call to ``join()``
            Directories are not handles properly yet

        Args:
            xfilename (str): filename of file to be extracted form archive
            outdir (str): directory to which to extract file to
            pipe (bool): extract to an automatically named fifo (named pipe)

        Returns:
            str: path to extracted filename/named pipe

        """
        if not os.path.isdir(outdir):
            raise OSError("Output directory does not exist {}".format(outdir))
        else:
            self._pipe = pipe
            self.outdir = outdir
            self._xfilename = xfilename
            filename = os.path.basename(xfilename)
            self.outpath = os.path.join(outdir, filename)
            if pipe:
                self.outpath = self._make_pipe(outdir)
            self._thread = threading.Thread(target=self._extract)
            self._thread.start()
        return self.outpath

    def extract_multiple(self, outdir, files=[]):
        """Extract multiple files from 7z archive to file system.

        Extract multiple files from 7z archive to the file system
        if ``files`` is not specified **all** files will be extracted

        Note:
            Directories are not handles properly yet

        Args:
            outdir (str): directory to which to extract file to
            files (list): list of files

        Returns:

        """
        if not files:
            files = self.list_files()
        for file_ in files:
            self.extract(file_, outdir)
            self.join()

    def join(self):
        """Wait for extraction thread to complete."""
        if self._thread:
            self._thread.join()
            self._thread = None
