import json
import urllib.request
import urllib.error


def fetch_kecamatan(kabupaten_id):
    base_url = "https://api.data.belajar.id/data-portal-backend/v1"
    url = f"{base_url}/master-data/satuan-pendidikan/statistics/{kabupaten_id}/descendants"

    params = {"sortBy": "bentuk_pendidikan", "sortDir": "asc"}

    # Construct query string
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"

    try:
        with urllib.request.urlopen(full_url) as response:
            data = json.loads(response.read().decode())
            return data
    except urllib.error.URLError as e:
        print(f"Error fetching data: {e}")
        return None


def main():
    kabupaten_id = "220000"  # Example kabupaten ID
    data = fetch_kecamatan(kabupaten_id)

    if data and "data" in data:
        print("\nKecamatan List:")
        for item in data["data"]:
            print(f"Kecamatan ID: {item.get('kode_wilayah')}, Name: {item.get('nama')}")


if __name__ == "__main__":
    main()
