import pandas as pd
from typing import Tuple, Dict

def read_portfolio(data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    import yfinance as yf
    """
    Parameters:
        data : pd.DataFrame
            A pandas DataFrame containing stock portfolio data. It must include a column 
            named 'symbol' with the stock ticker symbols.
    """
    tickers = {}
    
    for _, x in data.iterrows():
        tickers[x.symbol] = yf.Ticker(x.symbol)

    data['sector'] = data['symbol'].apply(lambda x : tickers.get(x).info.get('sector'))

    data['industry'] = data['symbol'].apply(lambda x : tickers.get(x).info.get('industry'))

    data['current'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('current'))

    data['investment_value'] = data['current'] * data['shares']

    data['low'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('low'))

    data['high'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('high'))

    data['mean'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('mean'))

    data['median'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('median'))

    data['strongBuy'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['strongBuy'])

    data['buy'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['buy'])

    data['hold'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['hold'])

    data['sell'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['sell'])

    data['strongSell'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['strongSell'])

    data['cik'] = data['symbol'].apply(lambda x : get_cik_from_symbol(x))

    return data, tickers

def get_cik_from_symbol(symbol: str) -> str:
    """
    Retrieves the CIK (Central Index Key) for a given stock symbol using the SEC EDGAR API.

    Parameters:
        symbol (str): The stock ticker symbol.

    Returns:
        str: The corresponding CIK or an error message if not found.
    """
    import requests
    import os
    import json
    from datetime import datetime, timedelta
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
                        return str(item['cik_str']).zfill(10)
                    
        url = f"https://www.sec.gov/files/company_tickers.json"
        headers = {
            'User-Agent': 'Ahmed Fadlalla (ahmedmaaf@gmail.com)',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        with open(cache_file, 'w') as file:
            json.dump(data, file, indent=4)

        for item in data.values():
            if item['ticker'].upper() == symbol:
                return str(item['cik_str']).zfill(10)

        return "CIK not found for the provided symbol."

    except requests.RequestException as e:
        return f"An error occurred while accessing the SEC EDGAR API: {e}"

def get_company_submission(cik: str) -> dict:
    """
    Retrieves all submissions filed by a company from the SEC EDGAR API.
    Saves the data to a JSON file and updates it if older than 7 days.

    Parameters:
        cik (str): The CIK of the company.

    Returns:
        dict: A dictionary containing all submissions or an error message if retrieval fails.
    """
    import requests
    import os
    import json
    from datetime import datetime, timedelta
    try:
        file_path = f"Submssions/CIK{cik}_submissions.json"
        # Check if the file exists and was updated less than 7 days ago
        if os.path.exists(file_path):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if datetime.now() - file_mod_time < timedelta(days=7):
                with open(file_path, 'r') as file:
                    return json.load(file)

        # The SEC EDGAR API URL for company submissions
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"

        headers = {
            'User-Agent': 'Your Name (your.email@example.com)',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        }

        # Make the API request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the JSON response
        submissions = response.json()

        # Save the data to a file
        with open(file_path, 'w') as file:
            json.dump(submissions, file, indent=4)

        return submissions

    except requests.RequestException as e:
        return {"error": f"An error occurred while accessing the SEC EDGAR API: {e}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {e}"}

def get_company_facts(cik: str, symbol: str) -> dict:
    """
    Retrieves company facts from the SEC EDGAR API and saves it to a JSON file named after the symbol.
    If the file exists and was updated less than a day ago, return the data from the file.

    Parameters:
        cik (str): The CIK of the company.
        symbol (str): The stock ticker symbol.

    Returns:
        dict: The company facts or an error message if retrieval fails.
    """
    import requests
    import os
    import json
    from datetime import datetime, timedelta
    try:
        file_path = f"companiesFacts/{symbol}.json"
        if os.path.exists(file_path):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if datetime.now() - file_mod_time < timedelta(days=10):
                with open(file_path, 'r') as file:
                    return json.load(file)

        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        
        headers = {
            'User-Agent': 'Ahmed Fadlalla (ahmedmaaf@gmail.com)',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
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

def get_fact(company_facts: dict, fact: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extracts and organizes quarterly and annual revenue data from a company's financial facts.

    Parameters:
        company_facts (dict): A dictionary containing financial data for a company, structured
                              as per the SEC EDGAR API's `companyfacts` endpoint.
    """

    company_facts_df = pd.DataFrame(company_facts['facts']['us-gaap'][fact]['units']['USD'])
    company_facts_df['start'] = pd.to_datetime(company_facts_df['start'])
    company_facts_df['end'] = pd.to_datetime(company_facts_df['end'])
    company_facts_df['diff'] = company_facts_df['end'] - company_facts_df['start']

    annual_facts = company_facts_df[company_facts_df['form'] == '10-K']
    annual_facts = annual_facts.sort_values(by=['start', 'filed'], ascending=[True, False])

    quarter_facts = company_facts_df[company_facts_df['form'] == '10-Q']
    quarter_facts = quarter_facts.sort_values(by=['end', 'filed'], ascending=[True, False])

    annual_data = {
        'Date': [],
        'Value': []
    }
    quarter_data = {
        'Date': [],
        'Value': []
    }

    year_idx = {}

    annual_obtained = set()
    quarter_obtained = set()
    counter = 0
    for _, row in annual_facts.iterrows():
        if row['end'] in annual_obtained:
            continue
        elif row['diff'].days >= 363:
            annual_data['Date'].append(row['end'])
            annual_data['Value'].append(row['val'])
            annual_obtained.add(row['end'])
            year_idx[row['end'].year] = counter
            counter += 1


    for _, row in quarter_facts.iterrows():
        if row['end'] in quarter_obtained:
            continue
        elif row['diff'].days >= 89 and row['diff'].days <= 91:
            quarter_data['Date'].append(row['end'])
            quarter_data['Value'].append(row['val'])
            quarter_obtained.add(row['end'])


    annual_data = pd.DataFrame(annual_data)
    annual_data['Date'] = pd.to_datetime(annual_data['Date'])
    annual_data = annual_data.set_index('Date')

    quarter_data = pd.DataFrame(quarter_data, columns=['Date', 'Value'])
    quarter_data['Date'] = pd.to_datetime(quarter_data['Date'])
    quarter_data = quarter_data.set_index('Date')

    sum, counter = 0, 0
    prev = quarter_data.index[0].year
    for row in quarter_data.index:
        if row.year == prev:
            counter += 1
            sum += quarter_data.loc[row]['Value']
        else:
            prev = row.year
            sum, counter = quarter_data.loc[row]['Value'], 1
        if counter == 3:
            if len(annual_data[annual_data.index >= row]) > 0:
                value = annual_data[annual_data.index >= row].iloc[0]['Value']
                time = annual_data[annual_data.index >= row].iloc[0].name
                quarter_data.loc[time] = value - sum
            sum, counter = 0, 0
    quarter_data = quarter_data.sort_index()

    return (quarter_data, annual_data)

def get_news_content(link: str) -> str:
    """
    Retrieves the full news content from the provided link.

    Parameters:
        link (str): The URL of the news article.

    Returns:
        str: The full text content of the news article, or an error message if retrieval fails.
    """
    import requests
    from bs4 import BeautifulSoup
    try:
        # Fetch the news article page
        response = requests.get(link)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the main content (adjust the tag/class selection as needed)
        paragraphs = soup.find_all('p')
        news_content = "\n".join([p.get_text() for p in paragraphs])

        return news_content

    except requests.RequestException as e:
        return f"An error occurred while accessing the news link: {e}"
    except Exception as e:
        return f"An unexpected error occurred: {e}"





