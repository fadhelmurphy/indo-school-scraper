from dataclasses import dataclass
from typing import List


@dataclass
class Province:
    code: str
    name: str


@dataclass
class Kabupaten:
    code: str
    name: str
    province_code: str


@dataclass
class Kecamatan:
    code: str
    name: str
    kabupaten_code: str


@dataclass
class School:
    npsn: str
    name: str
    kecamatan_code: str
