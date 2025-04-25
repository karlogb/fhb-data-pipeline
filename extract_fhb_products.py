import requests, subprocess, json, base64
import polars as pl
from datetime import datetime
from pytz import timezone


def get_auth(api_name):
    """
    Retrieve authentication credentials for a given API.
    Args:
        api_name (str): The name of the API for which to retrieve authentication credentials.
    Returns:
        dict: A dictionary containing the authentication credentials for the specified API.
              For the "fhb" API, the dictionary contains "app_id" and "secret" keys with empty string values.
    """
    if api_name == "fhb":
        auth_dict = {"app_id": "", "secret": ""}
    return auth_dict


def _get_schema_from_json(json):
    """
    Generate a schema dictionary from a given JSON object.
    This function takes a JSON object, infers its schema using the `pl.from_dicts` method,
    and then converts the schema to a dictionary. It ensures that columns with `Null` or
    `Boolean` types are converted to `Utf8` type.
    Args:
        json (list): A list of dictionaries representing the JSON object.
    Returns:
        dict: A dictionary where keys are column names and values are their inferred types.
    """
    schema_dict = dict(pl.from_dicts(json, infer_schema_length=None).schema)
    schema = {}

    for name, type in schema_dict.items():
        if type == pl.Null:
            schema.update({name: pl.Utf8})
        elif type == pl.Boolean:
            schema.update({name: pl.Utf8})
        else:
            schema.update({name: type})
    return schema


def get_json_fhb_products(run_date, data_path, url, headers):
    """
    Fetches product data from the given URL and saves it as a JSON file.
    Args:
        run_date (datetime): The date of the run, used to create a unique directory for the output file.
        data_path (str): The base path where the JSON file will be saved.
        url (str): The base URL for the API endpoint.
        headers (dict): The headers to include in the API requests.
    Returns:
        None
    Raises:
        requests.exceptions.RequestException: If there is an issue with the API request.
        subprocess.CalledProcessError: If there is an issue creating the directory.
        IOError: If there is an issue writing the JSON file.
    Example:
        get_json_fhb_products(datetime.now(), '/path/to/data', 'https://api.example.com', {'Authorization': 'Bearer token'})
    """
    run_date_key = run_date.strftime("%Y%m%d")

    product_all = requests.get(f"{url}/product/all", headers=headers)
    product_all_cnt = product_all.json()["total"]
    product_all_pages = int(product_all_cnt / 250) + 1

    product_all_list = []

    for p in range(product_all_pages):
        product_page = requests.get(f"{url}/product/all?page={p+1}", headers=headers)
        product_all_list += product_page.json()["products"]

    if product_all_cnt == len(product_all_list):
        print("CHECK: Product count PASSED!")
    else:
        print("CHECK: Product count FAILED!")

    subprocess.check_output(
        f"mkdir -p {data_path}/landing/products/{run_date_key}", shell=True
    )
    json_file_path = (
        f"{data_path}/landing/products/{run_date_key}/" + "products_raw.json"
    )

    with open(json_file_path, "w") as json_file:
        json.dump(product_all_list, json_file, indent=2)
    print(
        f"Data has been successfully written to {json_file_path} with page_count: {product_all_pages}"
    )


def get_fhb_products(run_date, data_path, url, headers):
    """
    Fetches FHB products data, processes it, and saves it as a parquet file.
    Args:
        run_date (datetime): The date of the run, used to generate file paths.
        data_path (str): The base path where data files are stored.
        url (str): The URL to fetch data from (not used in the current implementation).
        headers (dict): Headers for the HTTP request (not used in the current implementation).
    Returns:
        None
    Side Effects:
        - Reads a JSON file containing product data.
        - Creates a directory for saving processed data.
        - Converts the JSON data to a DataFrame with an additional 'run_date_key' column.
        - Writes the DataFrame to a parquet file.
        - Prints a success message with the path and shape of the saved data.
    """
    run_date_key = run_date.strftime("%Y%m%d")
    json_file_path = (
        f"{data_path}/landing/products/{run_date_key}/" + "products_raw.json"
    )
    save_path = f"{data_path}/preprocess/products/{run_date_key}"

    with open(json_file_path, "r") as json_file:
        products = json.load(json_file)

    subprocess.check_output(f"mkdir -p {save_path}", shell=True)

    products_schema = _get_schema_from_json(products)
    products_df = pl.from_dicts(products, schema=products_schema).with_columns(
        pl.lit(run_date_key).alias("run_date_key")
    )
    products_df.write_parquet(f"{save_path}/products.parquet")

    print(
        f"Data has been successfully written to {save_path} with shape: {pl.from_dicts(products, infer_schema_length=None).shape}"
    )


def main():
    """
    Main function to preprocess landing data from FHB API.
    This function performs the following steps:
    1. Sets the API URL and data path.
    2. Gets the current date and time in the 'Europe/Bratislava' timezone.
    3. Authenticates with the FHB API and retrieves an authentication token.
    4. Encodes the token in base64 format.
    5. Sets up the headers for API requests.
    6. Calls functions to get JSON FHB products and FHB products.
    Raises:
        requests.exceptions.RequestException: If there is an issue with the API request.
        KeyError: If the 'token' key is not found in the login response.
    """
    url = "https://api.fhb.sk/v3"
    data_path = "/home/data/"
    run_date = datetime.now(timezone("Europe/Bratislava"))

    login = requests.post(f"{url}/login", data=json.dumps(get_auth("fhb")))
    token_raw = login.json()["token"]
    token = base64.b64encode(token_raw.encode("utf-8")).decode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "X-Authentication-Simple": token,
    }

    get_json_fhb_products(run_date, data_path, url, headers)
    get_fhb_products(run_date, data_path, url, headers)


if __name__ == "__main__":
    main()
