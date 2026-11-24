"""Generate small, deterministic, fully synthetic seed data for the demo.

No real or Marketplace rows are used: values come from a seeded RNG and the
curated lists below. Column names match the shape the staging models expect.

    python3 scripts/gen_seeds.py            # writes seeds/raw_*.csv
    python3 scripts/gen_seeds.py --as-of 2026-11-24

Service dates fall in the 90 days before --as-of (default: today), so the
demo looks current. Rerunning with the same --as-of produces identical files.
"""
import argparse
import csv
import datetime as dt
import random
from pathlib import Path

SEEDS = Path(__file__).resolve().parents[1] / "seeds"

N_PATIENTS, N_PROVIDERS, N_CLAIMS, N_CONDITIONS = 200, 25, 1000, 300

FIRST = ["Ava", "Ben", "Cara", "Dev", "Eli", "Fay", "Gus", "Hana", "Ivan", "Jo", "Kai", "Lena", "Max", "Nia", "Omar", "Pia"]
LAST = ["Adler", "Brook", "Cole", "Dunn", "Ellis", "Frost", "Gray", "Hale", "Irwin", "Jones", "Keane", "Lowe"]
CITIES = [("Springfield", "MA", "Hampden", "01103"), ("Riverton", "MD", "Howard", "21044"),
          ("Lakeside", "OH", "Erie", "44870"), ("Fairview", "TX", "Collin", "75069"),
          ("Hillcrest", "CA", "San Diego", "92103")]
RACES = ["white", "black", "asian", "native", "other"]
SPECIALTIES = ["GENERAL PRACTICE", "INTERNAL MEDICINE", "CARDIOLOGY", "PEDIATRICS", "ENDOCRINOLOGY"]
# (code, description, chronic) - SNOMED-style codes with plain-text descriptions
CONDITIONS = [
    ("44054006", "Diabetes mellitus type 2 (disorder)", True),
    ("59621000", "Essential hypertension (disorder)", True),
    ("195967001", "Asthma (disorder)", True),
    ("40055000", "Chronic sinusitis (disorder)", True),
    ("714628002", "Prediabetes (finding)", True),
    ("195662009", "Acute viral pharyngitis (disorder)", False),
    ("444814009", "Viral sinusitis (disorder)", False),
    ("10509002", "Acute bronchitis (disorder)", False),
    ("65966004", "Fracture of forearm (disorder)", False),
    ("43878008", "Streptococcal sore throat (disorder)", False),
]
STATUSES = ["BILLED", "CLOSED"]


def write(name, header, rows):
    SEEDS.mkdir(exist_ok=True)
    with open(SEEDS / f"{name}.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    print(f"seeds/{name}.csv: {len(rows)} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", type=dt.date.fromisoformat, default=dt.date.today())
    as_of = ap.parse_args().as_of
    rng = random.Random(20261124)

    patients = []
    for i in range(1, N_PATIENTS + 1):
        city, state, county, zip_code = rng.choice(CITIES)
        birth = dt.date(1940, 1, 1) + dt.timedelta(days=rng.randrange(0, 365 * 80))
        death = (as_of - dt.timedelta(days=rng.randrange(30, 2000))).isoformat() if rng.random() < 0.03 else ""
        patients.append([f"P{i:05d}", birth.isoformat(), death, rng.choice(FIRST), rng.choice(LAST),
                         rng.choice("MF"), rng.choice(RACES), rng.choice(["hispanic", "nonhispanic"]),
                         city, state, county, zip_code,
                         round(rng.uniform(5_000, 900_000), 2), round(rng.uniform(1_000, 400_000), 2)])
    write("raw_patients", ["PATIENT_ID", "BIRTHDATE", "DEATHDATE", "FIRST", "LAST", "GENDER", "RACE", "ETHNICITY",
                           "CITY", "STATE", "COUNTY", "ZIP", "HEALTHCARE_EXPENSES", "HEALTHCARE_COVERAGE"], patients)

    providers = []
    for i in range(1, N_PROVIDERS + 1):
        city, state, _, zip_code = rng.choice(CITIES)
        providers.append([f"D{i:04d}", f"ORG{(i - 1) // 5 + 1:03d}", f"Dr. {rng.choice(FIRST)} {rng.choice(LAST)}",
                          rng.choice("MF"), rng.choice(SPECIALTIES), f"{rng.randrange(1, 999)} Main St",
                          city, state, zip_code, rng.choice([0, 40, 150, 600, 1200]), rng.randrange(0, 800)])
    write("raw_providers", ["PROVIDER_ID", "ORGANIZATION_ID", "NAME", "GENDER", "SPECIALITY", "ADDRESS",
                            "CITY", "STATE", "ZIP", "ENCOUNTERS", "PROCEDURES"], providers)

    # Skewed: a few heavy users so dim_cms__patients shows every utilization tier
    weights = [60 if i < 2 else 25 if i < 6 else 1 for i in range(N_PATIENTS)]
    claims = []
    for i in range(1, N_CLAIMS + 1):
        pid = rng.choices(patients, weights)[0][0]
        prov = rng.choice(providers)[0]
        svc = dt.datetime.combine(as_of - dt.timedelta(days=rng.randrange(0, 90)), dt.time(rng.randrange(8, 18)))
        dx = [c[0] for c in rng.sample(CONDITIONS, rng.randrange(1, 4))] + [""] * 8
        amt1 = round(rng.uniform(0, 2_500), 2)
        claims.append([100000 + i, pid, prov, f"INS{rng.randrange(1, 9)}", "", rng.randrange(1, 20), rng.randrange(1, 20),
                       *dx[:8], rng.choice(providers)[0], f"E{i:06d}",
                       (svc - dt.timedelta(days=rng.randrange(0, 10))).isoformat(sep=" "), svc.isoformat(sep=" "), prov,
                       rng.choice(STATUSES), "", "", amt1, round(amt1 * 0.2, 2) if rng.random() < 0.3 else "", 0,
                       (svc + dt.timedelta(days=5)).isoformat(sep=" "), "", "", 1, ""])
    write("raw_claims", ["CLAIM_ID", "PATIENT_ID", "PROVIDER_ID", "PRIMARY_PATIENT_INSURANCE_ID",
                         "SECONDARY_PATIENT_INSURANCE_ID", "DEPARTMENTID", "PATIENTDEPARTMENTID",
                         *[f"DIAGNOSIS{n}" for n in range(1, 9)], "REFERRING_PROVIDER_ID", "ENCOUNTER_ID",
                         "CURRENTILLNESSDATE", "SERVICEDATE", "SUPERVISING_PROVIDER_ID", "STATUS1", "STATUS2", "STATUSP",
                         "OUTSTANDING1", "OUTSTANDING2", "OUTSTANDINGP", "LASTBILLEDDATE1", "LASTBILLEDDATE2",
                         "LASTBILLEDDATEP", "HEALTHCARECLAIMTYPEID1", "HEALTHCARECLAIMTYPEID2"], claims)

    conditions = []
    for i in range(1, N_CONDITIONS + 1):
        code, desc, chronic = rng.choice(CONDITIONS)
        start = as_of - dt.timedelta(days=rng.randrange(0, 720))
        stop = "" if chronic else (start + dt.timedelta(days=rng.randrange(5, 30))).isoformat()
        conditions.append([i, start.isoformat(), stop, rng.choice(patients)[0], code, desc])
    write("raw_conditions", ["CONDITION_ID", "START_DATE", "STOP_DATE", "PATIENT", "CODE", "DESCRIPTION"], conditions)


if __name__ == "__main__":
    main()
