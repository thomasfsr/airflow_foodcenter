import io
import os

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

def create_data_dir(data_folder: str = "data"):
    """Creates a data folder to keep the parquet tables from Google Drive."""
    os.makedirs(data_folder, exist_ok=True)


def download_from_gdrive(data_folder: str = "data"):
    """Downloads from the google drive folder the csv files."""
    data_folder = "data"
    folder_id = os.environ.get("folder_id")
    credentials = service_account.Credentials.from_service_account_file(
        "credentials/api_key.json"
    )

    drive_service = build("drive", "v3", credentials=credentials)

    files = (
        drive_service.files()
        .list(q=f"'{folder_id}' in parents and mimeType='text/csv'")
        .execute()
    )
    for file in files.get("files", []):
        file_id = file["id"]
        filename = file["name"]
        path_plus_filename = os.path.join(data_folder, filename)

        if not os.path.isfile(path_plus_filename):
            type = filename.split(".")[-1]
            if type == "csv":
                request = drive_service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = io.BytesIO(request.execute())
                try:
                    with open(os.path.join(data_folder, filename), "wb") as f:
                        f.write(downloader.read())
                except:
                    print('doesnt export')
            else:
                print(f"File type **{type}** not supported.")

if __name__ == '__main__':
    create_data_dir()
    download_from_gdrive()