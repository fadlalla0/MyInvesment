def get_cik_from_symbol(symbol: str) -> str:
    import requests
    import os
    import json
    from datetime import datetime, timedelta
    """
    Retrieves the CIK (Central Index Key) for a given stock symbol using the SEC EDGAR API.

    Parameters:
        symbol (str): The stock ticker symbol.

    Returns:
        str: The corresponding CIK or an error message if not found.
    """
    try:
        cache_file = "company_tickers.json"
        symbol = symbol.upper()

        if os.path.exists(cache_file):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_mod_time < timedelta(days=30):
                with open(cache_file, 'r') as file:
                    data = json.load(file)
                for item in data.values():
                    if item['ticker'].upper() == symbol:
                        return '000' + str(item['cik_str'])
                    
        url = f"https://www.sec.gov/files/company_tickers.json"
        headers = {
            'User-Agent': 'Ahmed Fadlalla (ahmedmaaf@gmail.com',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        with open(cache_file, 'w') as file:
            json.dump(data, file, indent=4)

        for item in data.values():
            if item['ticker'].upper() == symbol:
                return '000' + str(item['cik_str'])

        return "CIK not found for the provided symbol."

    except requests.RequestException as e:
        return f"An error occurred while accessing the SEC EDGAR API: {e}"
    
def get_company_facts(cik: str, symbol: str) -> dict:
    import requests
    import os
    import json
    from datetime import datetime, timedelta
    """
    Retrieves company facts from the SEC EDGAR API and saves it to a JSON file named after the symbol.
    If the file exists and was updated less than a day ago, return the data from the file.

    Parameters:
        cik (str): The CIK of the company.
        symbol (str): The stock ticker symbol.

    Returns:
        dict: The company facts or an error message if retrieval fails.
    """
    try:
        file_path = f"companiesFacts/{symbol}.json"
        if os.path.exists(file_path):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if datetime.now() - file_mod_time < timedelta(days=1):
                with open(file_path, 'r') as file:
                    return json.load(file)

        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        
        headers = {
            'User-Agent': 'Ahmed Fadlalla (ahmedmaaf@gmail.com',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        company_facts = response.json()

        with open(file_path, 'w') as file:
            json.dump(company_facts, file, indent=4)

        return company_facts

    except requests.RequestException as e:
        return {"error": f"An error occurred while accessing the SEC EDGAR API: {e}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {e}"}









