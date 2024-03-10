def get_product_category_pairs(products_df, categories_df, relations_df):
    # Joining products with their categories
    product_category_df = products_df \
        .join(relations_df,
              products_df["product_id"] == relations_df["product_id"],
              "inner") \
        .join(categories_df,
              relations_df["category_id"] == categories_df["category_id"],
              "inner") \
        .select(products_df["product_name"], categories_df["category_name"])

    # Getting products with no categories
    products_with_no_categories_df = products_df \
        .join(relations_df,
              products_df["product_id"] == relations_df["product_id"],
              "left_anti") \
        .select(products_df["product_name"])

    return product_category_df, products_with_no_categories_df
