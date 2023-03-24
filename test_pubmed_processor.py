import os
import unittest
import configparser

from ftp_downloader import FtpDownloader
from pubmed_processor import PubmedProcessor

"""
Covers some basic unit tests for the project
"""

class TestPubMedProcessor(unittest.TestCase):

    def setUp(self):        
        # Load config file
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.config_dict = {section: dict(config.items(section)) for section in config.sections()}
        
        # Create an instance of FtpDownloader
        self.ftpDownloader = FtpDownloader.load_from_config(self.config_dict)
        
        # Create an instance of PubMedProcessor
        self.pubmedProcessor = PubmedProcessor()

    def test_download_and_convert_to_xml_files(self): 
        """Covers the test for download_and_convert_to_xml_files() function
        Download the specific number of gz files and convert them to xml. 
        Files from the ftp folder will be ordered in descending order first before downloading the specific number"""      
        files_to_download_and_convert = 2
        # Download latest 2 gz files from pubmed ftp and convert to XML, used for initial pubmed xml analysis
        converted_files = self.ftpDownloader.download_and_convert_to_xml_files(files_to_download_and_convert)

        # Assert that converted file count is what we expect
        self.assertEqual(len(converted_files), files_to_download_and_convert)

    def test_download_file(self):   
        """Downloads a single gz file"""
        gz_file_to_download = 'pubmed23n0002.xml.gz'

        # Download latest 2 gz files from pubmed ftp and convert to XML, used for initial pubmed xml analysis
        self.ftpDownloader.download_file(gz_file_to_download)

        download_dir = self.config_dict['pubmedftp']['download_dir']
        # Assert that requested file is downloaded to the download directory
        self.assertTrue(os.path.exists(os.path.join(download_dir, gz_file_to_download)))    

    def test_download_file_and_convert_to_xml(self): 
        """Download the provided gz file and convert it to xml"""       
        xml_file_to_download = 'pubmed23n0002.xml.gz'

        # Download latest 2 gz files from pubmed ftp and convert to XML, used for initial pubmed xml analysis
        self.ftpDownloader.download_file_and_convert_to_xml(xml_file_to_download)

        download_dir = self.config_dict['pubmedftp']['download_dir']
        # Assert that requested file is downloaded to the download directory
        self.assertTrue(os.path.exists(os.path.join(download_dir, xml_file_to_download)))        

    def test_download_latest_files_by_count(self):
        """Download the specific number of gz files. 
        Files from the ftp folder will be ordered in descending order first before downloading the specific number""" 
                
        number_of_files_to_download = 10
        # Download latest 10 gz files from pubmed ftp, used for initial pubmed xml analysis
        downloaded_files = self.ftpDownloader.download_latest_files_by_count(number_of_files_to_download)

        # Assert that downloaded file count is what we expect
        self.assertEqual(len(downloaded_files), number_of_files_to_download)

    def test_download_files(self):             
        """Download the gz files provided in the filesToDownload parameter """                        
        files_to_download = ['pubmed23n0001.xml.gz', 'pubmed23n0002.xml.gz']
        # Download latest 10 gz files from pubmed ftp, used for initial pubmed xml analysis
        downloaded_files = self.ftpDownloader.download_files(files_to_download)

        # Assert that downloaded file count is what we expect
        self.assertEqual(len(downloaded_files), len(files_to_download))

    def test_index_pubmed_files(self):
        """Indexes all the gz files in the provided list_of_files parameter"""
        local_dir = self.config_dict['elasticsearch']['local_dir']
        print(f'local_dir={local_dir}')
        files_to_index = [os.path.join(local_dir, 'pubmed23n1114.xml.gz'), os.path.join(local_dir, 'pubmed23n1115.xml.gz')]        
        #Following code is to process and index all the gz files in the pubmed_folder
        files_indexed = self.pubmedProcessor.index_pubmed_files(files_to_index)
        self.assertEqual(len(files_to_index), files_indexed)

    def test_index_all_pubmed_files(self):
        """Indexes all the gz files available in the provided pubmed_folder"""        
        pubmed_folder = self.config_dict['elasticsearch']['local_dir'] #'/Users/muhammadayub/Documents/PubmedData/test'
        files_to_index = self.pubmedProcessor.get_all_gz_files(pubmed_folder)
        #Following code is to process and index all the gz files in the pubmed_folder
        files_indexed = self.pubmedProcessor.index_all_pubmed_files(pubmed_folder)
        self.assertEqual(len(files_to_index), files_indexed)


if __name__ == '__main__':
    unittest.main()
