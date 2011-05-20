#!/usr/bin/env python2.7

# To debug this, python2.7 -m pdb myscript.py

import re

class TcgaMetadata:
    '''
    This class is responsible for pulling metadata from TCGA and making it accessible for workflows, etc...
    '''
    def __init__(self, url):

        urlPattern = re.compile(r"^((http[s]?):\/)#?\/?([^:\/\s]+)((\/[\w\-\.]+)*\/)*([\w\-\.]+[^#?\s]+)")
        match = urlPattern.match(url)
        if(match):
            self.protocol = match.group(1)
            self.host = match.group(3)
            self.path = match.group(4)
            self.file = match.group(6)

        if(re.search('/bcr/', url)):
               self.type = 'C'
        elif(re.search('/cgcc/', url)):
               self.type = 'E'
            
    def getPlatform(self):
        parts = re.split('/', self.path)
        return parts[-3]

    def getName(self):
        parts = re.split('.tar.gz', self.file)
        return parts[0]

    def getType(self):
        return self.type

    def getFilename(self):
        return self.file
    
#------- UNIT TESTS -----------------
if __name__ == '__main__':
    import unittest

    class TestTcgaMetadata(unittest.TestCase):

        def setUp(self):
            self.meta = TcgaMetadata('http://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/coad/cgcc/unc.edu/agilentg4502a_07_3/transcriptome/unc.edu_COAD.AgilentG4502A_07_3.Level_2.2.0.0.tar.gz')

        def test_init(self):
            self.assertEqual('http:/', self.meta.protocol)
            self.assertEqual('tcga-data.nci.nih.gov', self.meta.host)
            self.assertEqual('/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/coad/cgcc/unc.edu/agilentg4502a_07_3/transcriptome/', self.meta.path)
            self.assertEqual('unc.edu_COAD.AgilentG4502A_07_3.Level_2.2.0.0.tar.gz', self.meta.file)

        def test_getPlatform(self):
            self.assertEqual('agilentg4502a_07_3', self.meta.getPlatform())

        def test_getName(self):
            self.assertEqual('unc.edu_COAD.AgilentG4502A_07_3.Level_2.2.0.0', self.meta.getName())

        def test_getType(self):
            self.assertEqual('E', self.meta.getType())

        def test_getFilename(self):
            self.assertEqual('unc.edu_COAD.AgilentG4502A_07_3.Level_2.2.0.0.tar.gz', self.meta.getFilename())

            
    unittest.main()
