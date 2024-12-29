from typing import Dict, Any, Optional
from google.cloud import storage
from utils.file_naming import generate_filename
from data_fetchers.fetcher_factory import create_fetcher


class DataProcessor:
    def __init__(
        self,
        project_id: str,
        gcs_bucket: str,
        gcs_folder: str,
        job_id: int,
        business_id: str,
        data_type: str,
        data_id: Optional[str] = None,
        district_cd: Optional[str] = None,
    ):
        self.project_id = project_id
        self.gcs_bucket = gcs_bucket
        self.gcs_folder = gcs_folder
        self.job_id = job_id
        self.business_id = business_id
        self.data_type = data_type
        self.data_id = data_id
        self.district_cd = district_cd
        # self.storage_client = storage.Client(project=project_id)

    def process(self) -> Dict[str, Any]:
        # Create appropriate fetcher based on data type
        fetcher = create_fetcher(self.data_type)

        # Fetch data
        data = fetcher.fetch(self.data_id)

        # Generate filename
        filename = generate_filename(
            self.data_type, self.data_id, self.district_cd, self.job_id
        )
        print(data, "<< data")

        return {
            "status": "success",
            "filename": filename,
            "path": f"gs://{self.gcs_bucket}/{self.gcs_folder}/{filename}",
            "record_count": len(data),
        }

        # Upload to GCS
        bucket = self.storage_client.bucket(self.gcs_bucket)
        blob = bucket.blob(f"{self.gcs_folder}/{filename}")

        # Convert data to CSV string
        csv_content = fetcher.to_csv(data)
        blob.upload_from_string(csv_content, content_type="text/csv")

        return {
            "status": "success",
            "filename": filename,
            "path": f"gs://{self.gcs_bucket}/{self.gcs_folder}/{filename}",
            "record_count": len(data),
        }
