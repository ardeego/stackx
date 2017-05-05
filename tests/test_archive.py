import unittest
import os
from stackx.archive import Archive7z

class TestAchive7zaMethods(unittest.TestCase):

    tables = [
        'Badges.xml',
        'Comments.xml',
        'PostHistory.xml',
        'PostLinks.xml',
        'Posts.xml',
        'Tags.xml',
        'Users.xml',
        'Votes.xml'
    ]

    extra = [
        'test',
        'test/README'
    ]

    readme = "this is a test file used for unit testing...\n"

    def setUp(self):
        with self.assertRaises(OSError):
            Archive7z("data/test.stackexchange.com.7z", cmd="binary/does/not/exist/7z")
        self.archive = Archive7z("data/test.stackexchange.com.7z")

    def test_list_files(self):
        #Test file listing
        list_ = self.tables + self.extra
        list_ = list_.sort()
        self.assertEqual(self.archive.list_files().sort(), list_)
        list_ = self.tables.sort()
        self.assertEqual(self.archive.list_files("xml").sort(), list_)
        self.assertEqual(self.archive.list_files(".xml").sort(), list_)

    def test_extract(self):
        #Test extraction to named pipe
        filename = self.archive.extract(self.extra[1], "data", pipe=True)
        self.assertTrue(os.path.exists(filename), "Failed to create file" )
        with open(filename, "r", encoding="utf8") as fifo:
            data = fifo.readlines()
        self.archive.join()
        self.assertEqual("".join(data), self.readme, "Extracted does not match expected")
        self.archive.join()
        self.assertFalse(os.path.exists(filename), "Failed to clean up named pipe" )

        #Test extraction to disk
        filename = self.archive.extract(self.tables[0], "data")
        self.archive.join()
        self.assertEqual(os.path.join("data", self.tables[0]), filename, "Failed to create file")
        with open(filename, "r", encoding="utf8") as file_:
            data = file_.readlines()
        self.assertTrue("Autobiographer" in "".join(data), "Extracted does not match expected")
        self.archive.join()
        os.remove(filename)

if __name__ == "__main__":
    unittest.main()
