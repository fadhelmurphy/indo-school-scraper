import csv
from typing import List
from models import Province, Kabupaten, Kecamatan, School


class DataWriter:
    def write_provinces(self, provinces: List[Province], filename: str):
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["code", "name"])
            writer.writerows([[p.code, p.name] for p in provinces])

    def write_kabupatens(self, kabupatens: List[Kabupaten], filename: str):
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["code", "name", "province_code"])
            writer.writerows([[k.code, k.name, k.province_code] for k in kabupatens])

    def write_kecamatans(self, kecamatans: List[Kecamatan], filename: str):
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["code", "name", "kabupaten_code"])
            writer.writerows([[k.code, k.name, k.kabupaten_code] for k in kecamatans])

    def write_schools(self, schools: List[School], filename: str):
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["npsn", "name", "kecamatan_code"])
            writer.writerows([[s.npsn, s.name, s.kecamatan_code] for s in schools])
