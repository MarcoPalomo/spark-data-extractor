from src.transformers.data_transformer import DataTransformer


def test_select_and_cast(spark):
    df = spark.createDataFrame([
        (1, "10", None),
        (2, "20", 3.14),
    ], ["id", "amount_str", "misc"])

    tr = DataTransformer()
    df2 = tr.select_columns(df, ["id", "amount_str"])  # drop misc
    assert set(df2.columns) == {"id", "amount_str"}

    df3 = tr.cast_columns(df2, {"amount_str": "int"})
    assert dict(df3.dtypes)["amount_str"] in ("int", "int32", "int64")


def test_dropnulls_and_deduplicate(spark):
    df = spark.createDataFrame([
        (1, "A"),
        (1, "A"),
        (2, None),
        (3, "C"),
    ], ["id", "val"])

    tr = DataTransformer()
    df2 = tr.drop_nulls(df, subset=["val"])  # drop row with None val
    assert df2.count() == 3

    df3 = tr.deduplicate(df2, subset=["id", "val"])  # drop exact duplicate
    assert df3.count() == 2

