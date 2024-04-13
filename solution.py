from pyspark.sql import DataFrame, SparkSession


def get_product_and_category_names(products: DataFrame, categories: DataFrame,
                                   relations: DataFrame) -> DataFrame:
    joined_df = products.join(relations, "product_id", "left_outer").join(
        categories, "category_id", "left_outer")
    result_df = joined_df.select("product_name", "category_name")

    return result_df


# Preparing test data
products_raw = [
    (1, "product_name1"), (2, "product_name2"),
    (3, "product_name3"), (4, "product_name4"),
    (5, "product_name5"), (6, "product_name6")
]
categories_raw = [
    (1, "category_name1"), (2, "category_name2"), (3, "category_name3")
]
relations_raw = [(1, 2), (2, 1), (3, 6), (2, 4), (1, 1)]

# Testing..
spark = SparkSession.builder.appName("Test_task").getOrCreate()

# Creating DataFrames
products_df = spark.createDataFrame(
    products_raw, ["product_id", "product_name"]
)
categories_df = spark.createDataFrame(
    categories_raw, ["category_id", "category_name"]
)
relations_df = spark.createDataFrame(
    relations_raw, ["category_id", "product_id"]
)

# Let's see what we've done
products_df.show()
categories_df.show()
relations_df.show()

# Testing func
result = get_product_and_category_names(products_df, categories_df,
                                        relations_df)
result.show()
