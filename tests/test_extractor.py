import json
from pathlib import Path

from src.extractors.data_extractor import DataExtractor


def test_extract_csv(spark, tmp_path: Path):
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, schema=["id", "val"])
    csv_dir = tmp_path / "csv"
    df.write.mode("overwrite").option("header", True).csv(str(csv_dir))

    extractor = DataExtractor(spark)
    out = extractor.extract_from_source({
        "type": "csv",
        "path": str(csv_dir),
        "options": {"header": True, "inferSchema": True}
    })
    assert out.count() == 2


def test_extract_json(spark, tmp_path: Path):
    json_dir = tmp_path / "json"
    json_dir.mkdir(parents=True, exist_ok=True)
    records = [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]
    with open(json_dir / "data.json", "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    extractor = DataExtractor(spark)
    out = extractor.extract_from_source({
        "type": "json",
        "path": str(json_dir)
    })
    assert out.count() == 2


def test_extract_parquet(spark, tmp_path: Path):
    data = [(1, "X"), (2, "Y"), (3, "Z")]
    df = spark.createDataFrame(data, schema=["k", "v"])
    pq_dir = tmp_path / "pq"
    df.write.mode("overwrite").parquet(str(pq_dir))

    extractor = DataExtractor(spark)
    out = extractor.extract_from_source({
        "type": "parquet",
        "path": str(pq_dir)
    })
    assert out.count() == 3

