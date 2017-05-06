"""A lightweight wrapper for MySQLdb connections."""
import os
import MySQLdb
import logging
from .archive import Archive7z

log = logging.getLogger(__name__)

class Connection(object):
    """A lightweight wrapper for MySQLdb connections."""

    conn = None

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
        """(Re-)connect to data base."""
        self.close()
        try:
            self.conn = MySQLdb.connect(**self._args)
            self.conn.autocommit(self._autocommit)
        except MySQLdb.Error as e:
            log.error("Failed to establish connection to MySQL on %s", self.host,
                      exc_info=True)

    def close(self):
        """Close connection to the stackexchange database."""
        if self.conn:
            self.conn.join()
            self.conn = None

    def import_7z_archive(self, filename, dbname=None):
        """Import stackexchange.com database from 7z archive"""
        if not dbname:
            _, dbname  = os.path.split(filename)
            dbname  = os.path.split(dbname)


        archive = Archive7z(filename)
        for xfilename in archive.list_files("xml"):
            pipename = archive.extract(xfilename, outdir=self.secure_file_priv , pipe=True)

    def import_xml_table(self, xml_filename):
        """Import xml table dump file.

        Args:
            xml_file_name (str): filename of xml files
        """
        if xml_filename:
            if not self.db:
                #self.db =
                pass

    def iter(self, query, *parameters, **kwparameters):
        """Return an iterator for the given query and parameters."""
        cursor = self.conn.cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *parameters, **kwparameters):
        """Return a row list for the given query and parameters."""
        cursor = self.conn.cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def get_one(self, query, *parameters, **kwparameters):
        """Return the (singular) row returned by the given query.

        Note:
            If the query has no results, returns None.
            For more than more than one result, raises an exception.
        """
        rows = self.query(query, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *parameters, **kwparameters):
        """Execute the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Execute the given query, returning the lastrowid from the query."""
        cursor = self.conn.cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Execute the given query, returning the rowcount from the query."""
        cursor = self.conn.cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Execute the given query against all the given param sequences.

        Returns:
           lastrowid from the query
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Execute the given query against all the given param sequences.

        Returns:
            lastrowid from the query
        """
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """Execute the given query against all the given param sequences.

        Returns:
            rowcount from the query
        """
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            return cursor.execute(query, kwparameters or parameters)
        except MySQLdb.OperationalError:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        """Return named field."""
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
