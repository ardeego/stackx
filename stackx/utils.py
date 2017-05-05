"""Misc supporting functions."""

import os

def is_exe(fpath):
    """Test if file is an executable.

    Args:
        fpath (str): path to binary

    Returns:

    """
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

def which(program):
    """Python implementaion of the UNIX ``which`` command.

    Args:
        program (str): name of executable to look for

    Returns:
        str: path to executable

    """
    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None
