"""A lightweight wrapper for MySQLdb connections."""

import MySQLdb
import logging
from contextlib import closing

log = logging.getLogger(__name__)

class Connection(object):
    """A lightweight wrapper for MySQLdb connections."""

    cnx = None

    def __init__(self, host=None, user=None, passwd=None, db="", charset="utf8",
                 autocommit=True, config=None, **kwargs):
        """Wrapper class around MySQl database.

        Note:
            A configuration pass via ``config`` parameter takes precedence
            over any other specified parameter

        Args:
            host (str): hostname, e.g. ``localhost:3306``
            user (str): username
            passwd (str): password
            db (str): database name
            charset (str): charset to be use, default: ``utf-8``
            autocommit (bool):
            config (dict): configuration in json type nested dict format
            **kwargs: additional parameters passed to MySQLdb connection object

        """
        self._autocommit = autocommit
        self.socket = None

        args  = dict(host=host, db=db, use_unicode=True, charset=charset, **kwargs)
        if config:
            config_ = config.copy()
            if "secure_file_priv" in config_:
                self.secure_file_priv = config_["secure_file_priv"]
            del config_["secure_file_priv"]
            args.update(config_)

        # Allow UNIX socket connections
        if "/" in args["host"]:
            args["unix_socket"] = args["host"]
            self.socket = args["host"]
            self.host = args["host"]
            del args["host"]
        else:
            self.host = args["host"]
            pair = args["host"].split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["port"] = 3306

        self.db = args["db"]
        self._args = args
        self.reconnect()

    def __del__(self):
        """Clean up."""
        self.close()

    def reconnect(self):
        """(Re-)connect to data base.

        Returns:

        """
        self.close()
        try:
            self.cnx = MySQLdb.connect(**self._args)
            self.cnx.autocommit(self._autocommit)
        except MySQLdb.Error as e:
            log.error("Failed to establish connection to MySQL on %s", self.host,
                      exc_info=True)

    def close(self):
        """Close connection to the stackexchange database.

        Returns:

        """
        if self.cnx:
            self.cnx.join()
            self.cnx = None


    def import_xml(self, xml_file_name):
        """Import xml dump file tor stackexchange table.

        Args:
            xml_file_name (str): filename of xml files

        Returns:

        """
        if xml_file_name:
            if not self.db:
                pass
