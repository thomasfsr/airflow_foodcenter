import os
import gdown
from typing import Dict
import time

files_list = {
    "channels.csv": "https://drive.google.com/file/d/1wwBnWMfwR5RJZVZULHaWQALrclru-Oyp",
    "payments.csv": "https://drive.google.com/file/d/1xtchHGwpD8s5_MOVCeMj_cMO6CvYcmS3",
    "stores.csv": "https://drive.google.com/file/d/1k4pQ3zpNyCUqG2EL1AREGfBPmr5t_ZMK",
    "orders.csv": "https://drive.google.com/file/d/1kiLBFv6_bR1fv1BahfrTEp5itDut9mVE",
    "deliveries.csv": "https://drive.google.com/file/d/1ba9-21ppV1Nailp2pJ4RGPjJIJ143_xe",
    "drives.csv": "https://drive.google.com/file/d/1JZlvYNvD2eVxjrj33s2T54s-itJTvXvI",
    "hubs.csv": "https://drive.google.com/file/d/1CRRC8K2wHZiL2vdewjb90UT4EcBQQaN5",
}

class GdownDownloader:
    def __init__(self, files:Dict = files_list, output_folder:str='data', max_retries=3, retry_delay=1):
        self.files = files
        self.output_folder = output_folder
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def download_files(self):
        os.makedirs(self.output_folder, exist_ok=True)
        
        for filename, url in self.files.items():
            output_path = os.path.join(self.output_folder, filename)
            retries = 0
            success = False

            while not success and retries < self.max_retries:
                try:
                    gdown.download(url, output_path, quiet=False)
                    success = True
                except Exception as e:
                    print(f"Failed to download {filename}: {e}")
                    retries += 1
                    time.sleep(self.retry_delay)
            
            if not success:
                print(f"Failed to download {filename} after {self.max_retries} retries.")

output_folder = "data"

downloader = GdownDownloader(files_list, output_folder)
downloader.download_files()