import os
import polars as pl


def present_fhb(data_path):
    """
    Processes product data from parquet files, aggregates it, and saves the result.
    Args:
        data_path (str): The base directory path where the product data is stored and where the processed data will be saved.
    The function performs the following steps:
    1. Reads product data from parquet files located in the specified data_path.
    2. Aggregates the product data by grouping on several columns and calculating the minimum run_date_key.
    3. Adds a 'current_flag' column to indicate the most recent update for each product.
    4. Saves the processed product data to a new parquet file in the 'present/products' directory within the specified data_path.
    """
    parquet_file_path = data_path + "/preprocess/products/*/products.parquet"
    products_import = pl.read_parquet(parquet_file_path).unique()

    products = (
        products_import.group_by(
            "internal_code",
            "ean",
            "name",
            "photo",
            "archived",
            "stock_quantity",
            "free_quantity",
        )
        .agg(pl.min("run_date_key").alias("update_date_key"))
        .with_columns(
            pl.when(
                pl.max("update_date_key").over("internal_code")
                == pl.col("update_date_key")
            )
            .then(pl.lit("Y"))
            .alias("current_flag")
        )
    )

    save_path = f"{data_path}/present/products"
    os.makedirs(save_path, exist_ok=True)
    parquet_file_path = f"{save_path}/products.parquet"
    print(parquet_file_path)
    products.write_parquet(parquet_file_path)


def main():
    """
    Main function to present FHB data.
    This function sets the data path and calls the present_fhb function
    to process and present the FHB data.
    Parameters:
    None
    Returns:
    None
    """
    data_path = "/home/data/"

    present_fhb(data_path)


if __name__ == "__main__":
    main()
