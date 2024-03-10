import pytest
from pyspark.sql import SparkSession
from app.product_category_analysis import get_product_category_pairs


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest_spark_test") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_get_product_category_pairs(spark_session):
    # Example DataFrame for products, categories and relations
    products = [(1, "product1"), (2, "product2"), (3, "product3"),
                (4, "product4")]
    categories = [(1, "category1"), (2, "category2"), (3, "category3"),
                  (4, "category4")]
    products_categories = [(1, 1), (2, 2), (4, 5), (5, 4)]

    # Create DataFrame from data
    products_df = spark_session.createDataFrame(products,
                                                ["product_id", "product_name"])
    categories_df = spark_session.createDataFrame(categories,
                                                  ["category_id",
                                                   "category_name"])
    relations_df = spark_session.createDataFrame(products_categories,
                                                 ["product_id", "category_id"])

    # Call the method to get product-category pairs and products
    # with no categories
    product_category_df, products_with_no_categories_df = \
        get_product_category_pairs(products_df, categories_df, relations_df)

    # Convert results to lists for easier comparison
    product_category_pairs_list = product_category_df.collect()
    products_with_no_categories_list = products_with_no_categories_df.collect()

    # Check product-category pairs
    # Assuming 2 valid pairs
    assert len(product_category_pairs_list) == 2
    assert ("product1", "category1") in product_category_pairs_list
    assert ("product2", "category2") in product_category_pairs_list

    # Check products with no categories
    # Assuming 2 products with no categories
    assert len(products_with_no_categories_list) == 1
    assert ("product3",) in products_with_no_categories_list
