from services.data_processor import DataProcessor

processor = DataProcessor(
    project_id="idf-corp-dev",
    gcs_bucket="src_ext_data",
    gcs_folder="kemdikbud_satpen",
    job_id=20241227163638,
    business_id="satpen",
    data_type="kabupaten",
    data_id=None,
    district_cd=None,
)

result = processor.process()
