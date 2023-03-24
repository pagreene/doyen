import logging
from ftplib import FTP
import re
import os
import urllib.request
import gzip
import shutil


class FtpDownloader: 
    """
    Downloads the files through FTP
    """   
    def __init__(self, host, ftp_dir, download_dir, user_id=None, pwd=None, reg_ex=".*"):
        self.host = host
        self.ftp_dir = ftp_dir        
        self.download_dir = download_dir
        self.user_id = user_id
        self.pwd = pwd        
        self.reg_ex = reg_ex    

    @property
    def logger(self):
        return logging.getLogger(__name__)
   
    @staticmethod
    def load_from_config(config_dict):
        cls_name = "pubmedftp"
        host = config_dict[cls_name]["host"]
        ftp_dir = config_dict[cls_name]["ftp_dir"]
        download_dir = config_dict[cls_name]["download_dir"]        
        user_id = config_dict[cls_name].get("user_id", None)
        pwd = config_dict[cls_name].get("pwd", None)
        reg_ex = config_dict[cls_name].get("reg_ex", ".*")
        return FtpDownloader(host, ftp_dir, download_dir, user_id, pwd, reg_ex)

    def iterate(self, local_path):
        ftp = None
        re_obj = re.compile(self.reg_ex)
        # Make the local path and ignore if exists..
        os.makedirs(local_path, exist_ok=True)
        try:
            ftp = self.ftp_client
            # login
            if self.user_id is None:
                ftp.login()
            else:
                ftp.login(self.user_id, self.pwd)

            ftp.cwd(self.ftp_dir)
            file_names = ftp.nlst()

            for filename in filter(lambda f: re_obj.match(f) is not None, file_names):
                self.logger.info("Downloading {} ..".format(filename))
                yield self._download_file(ftp, filename, local_path)

        finally:
            if ftp is not None: ftp.quit()

    def __call__(self, local_path):
        return list(self.iterate(local_path))   

    
    def download_file_and_convert_to_xml(self, gzfilename):
        """Download the provided gz file and convert it to xml"""

        # Define the URL of the file to download
        url = f'https://{self.host}/{self.ftp_dir}/{gzfilename}'
        
        # Define the name of the file to save locally
        localgzfilename = os.path.join(self.download_dir, gzfilename)

        # Download the file from the URL and save it locally
        urllib.request.urlretrieve(url, localgzfilename)

        # Extract the contents of the compressed file to an XML
        with gzip.open(localgzfilename, 'rb') as gz_file:
            xml_content = gz_file.read().decode('utf-8')
        
        xmlfilename = os.path.splitext(os.path.basename(localgzfilename))[0]
        
        # Save the XML content to a file
        with open(xmlfilename, 'w') as xml_file:
            xml_file.write(xml_content)

        # Print a message indicating that the file has been saved
        print(f'downloaded {localgzfilename} and converted to {xmlfilename}')
    
    def download_and_convert_to_xml_files(self, numberOfFiles=10,  deleteGzFilesAfterConverting=False):
        """Download the specific number of gz files and convert them to xml. 
        Files from the ftp folder will be ordered in descending order first before downloading the specific number"""
        # converted files list
        converted_files = []

        # first download the gz files before converting to xml
        downloaded_files = self.download_files(numberOfFiles)
        download_dir = self.download_dir#'/Users/muhammadayub/Documents/PubmedData/test'

        # convert the downloaded files to XML format and delete the gz files
        for filename in downloaded_files:
            if filename.endswith('.gz'):
                with gzip.open(os.path.join(download_dir, filename), 'rb') as f_in:
                    with open(os.path.join(download_dir, filename[:-3]), 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        converted_files.append(filename[:-3])
                        print(f'converted {os.path.join(download_dir, filename)} to {os.path.join(download_dir, filename[:-3])}')
                if deleteGzFilesAfterConverting:
                    os.remove(os.path.join(download_dir, filename))
                    print(f'deleted {os.path.join(download_dir, filename)}')
            elif filename.endswith('.xml'):
                continue

        return converted_files
     
    def download_latest_files_by_count(self, numberOfFiles=10):      
        """Download the specific number of gz files. 
        Files from the ftp folder will be ordered in descending order first before downloading the specific number"""  
        # set up the FTP connection
        ftp = FTP(self.host)
        ftp.login()
        ftp.cwd(self.ftp_dir)

        # downloaded files list
        downloaded_files = []

        # get the first 10 file names and download them to the specified directory
        filenames = [name for name in ftp.nlst() if name.endswith('.gz')]
        
        # sort the files in reverse order by modification time
        filenames.sort(key=lambda x: ftp.sendcmd("MDTM " + x)[4:], reverse=True)
        downloaded_files = self.download_files(filenames[:numberOfFiles])

        return downloaded_files

    def download_files(self, filesToDownload):  
        """Download the gz files provided in the filesToDownload parameter """             
        # set up the FTP connection
        ftp = FTP(self.host)
        ftp.login()
        ftp.cwd(self.ftp_dir)

        # downloaded files list
        downloaded_files = []        
        
        download_dir = self.download_dir#/Users/muhammadayub/Documents/PubmedData/test'

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        for filename in filesToDownload:
            with open(os.path.join(download_dir, filename), 'wb') as f:
                #if filename.endswith('.gz'):
                ftp.retrbinary('RETR ' + filename, f.write)
                downloaded_files.append(filename)
                print(f'downloaded {os.path.join(download_dir, filename)}')
        
        return downloaded_files
    
    
    def download_file(self, filename): 
        """Downloads a single gz file"""
        self.download_files([filename])        