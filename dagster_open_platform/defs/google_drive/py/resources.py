import base64
import io
import json

from dagster import ConfigurableResource
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


class GoogleDriveResource(ConfigurableResource):
    """Resource for uploading files to Google Drive using a service account."""

    service_account_json_base64: str

    def upload_csv(self, csv_content: str, filename: str, folder_id: str) -> str:
        credentials_dict = json.loads(
            base64.b64decode(self.service_account_json_base64).decode("utf-8")
        )
        creds = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        media = MediaIoBaseUpload(
            io.BytesIO(csv_content.encode("utf-8")),
            mimetype="text/csv",
            resumable=False,
        )
        result = (
            service.files()
            .create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        return result["id"]
