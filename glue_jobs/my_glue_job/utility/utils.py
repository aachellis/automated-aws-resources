def get_data(spark, range):
    df = spark.range(range)

    return df