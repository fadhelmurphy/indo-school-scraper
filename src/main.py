from api_client import APIClient
from data_writer import DataWriter
from bigquery_client import BigQueryClient


def main():
    api_client = APIClient()
    data_writer = DataWriter()
    bq_client = BigQueryClient()

    # Get processed IDs from BigQuery
    processed_provinces = bq_client.get_processed_provinces()

    # Get all provinces
    print("Fetching provinces...")
    provinces = api_client.get_provinces()

    # Filter out already processed provinces
    new_provinces = [p for p in provinces if p.code not in processed_provinces]

    if new_provinces:
        print(f"Found {len(new_provinces)} new provinces")
        data_writer.write_provinces(new_provinces, "provinces.csv")
        bq_client.save_provinces(new_provinces)
    else:
        print("No new provinces to process")

    # Get processed kabupatens
    processed_kabupatens = bq_client.get_processed_kabupatens()

    # Get all kabupatens for unprocessed provinces
    all_kabupatens = []
    for province in provinces:
        if province.code not in processed_provinces:
            print(
                f"\nFetching kabupatens for province {province.name} ({province.code})..."
            )
            kabupatens = api_client.get_kabupatens(province.code)
            new_kabupatens = [
                k for k in kabupatens if k.code not in processed_kabupatens
            ]
            all_kabupatens.extend(new_kabupatens)
            if new_kabupatens:
                print(f"Found {len(new_kabupatens)} new kabupatens")
                bq_client.save_kabupatens(new_kabupatens)

    if all_kabupatens:
        data_writer.write_kabupatens(all_kabupatens, "kabupatens.csv")
        print(f"\nTotal new kabupatens: {len(all_kabupatens)}")

    # Get processed kecamatans
    processed_kecamatans = bq_client.get_processed_kecamatans()

    # Get all kecamatans for unprocessed kabupatens
    all_kecamatans = []
    for kabupaten in all_kabupatens:
        if kabupaten.code not in processed_kabupatens:
            print(
                f"\nFetching kecamatans for kabupaten {kabupaten.name} ({kabupaten.code})..."
            )
            kecamatans = api_client.get_kecamatans(kabupaten.code)
            new_kecamatans = [
                k for k in kecamatans if k.code not in processed_kecamatans
            ]
            all_kecamatans.extend(new_kecamatans)
            if new_kecamatans:
                print(f"Found {len(new_kecamatans)} new kecamatans")
                bq_client.save_kecamatans(new_kecamatans)

    if all_kecamatans:
        data_writer.write_kecamatans(all_kecamatans, "kecamatans.csv")
        print(f"\nTotal new kecamatans: {len(all_kecamatans)}")

    # Get processed schools
    processed_schools = bq_client.get_processed_schools()

    # Get all schools for unprocessed kecamatans
    all_schools = []
    for kecamatan in all_kecamatans:
        if kecamatan.code not in processed_kecamatans:
            print(
                f"\nFetching schools for kecamatan {kecamatan.name} ({kecamatan.code})..."
            )
            schools = api_client.get_schools(kecamatan.code)
            new_schools = [s for s in schools if s.npsn not in processed_schools]
            all_schools.extend(new_schools)
            if new_schools:
                print(f"Found {len(new_schools)} new schools")
                bq_client.save_schools(new_schools)

    if all_schools:
        data_writer.write_schools(all_schools, "schools.csv")
        print(f"\nTotal new schools: {len(all_schools)}")


if __name__ == "__main__":
    main()
