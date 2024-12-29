from flask import Flask, request, jsonify
from datetime import datetime
from .services.data_processor import DataProcessor
from .utils.validators import validate_payload
from .utils.exceptions import ValidationError

app = Flask(__name__)


@app.route("/api/process", methods=["POST"])
def process_data():
    try:
        payload = request.get_json()
        validate_payload(payload)

        processor = DataProcessor(
            project_id=payload["project_id"],
            gcs_bucket=payload["gcs_bucket"],
            gcs_folder=payload["gcs_folder"],
            job_id=payload["job_id"],
            business_id=payload["business_id"],
            data_type=payload["type"],
            data_id=payload["data_id"],
            district_cd=payload["district_cd"],
        )

        result = processor.process()
        return jsonify(result), 200

    except ValidationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": "Internal server error", "details": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
