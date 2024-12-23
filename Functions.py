import pandas as pd
from typing import Tuple, Dict
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from headers import headers
pio.templates.default = "plotly_dark"

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

    data['price_target_low'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('low'))

    data['price_target_high'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('high'))

    data['price_target_mean'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('mean'))

    data['price_target_median'] = data['symbol'].apply(lambda x : tickers.get(x).analyst_price_targets.get('median'))

    data['strongBuy'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['strongBuy'])

    data['buy'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['buy'])

    data['hold'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['hold'])

    data['sell'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['sell'])

    data['strongSell'] = data['symbol'].apply(lambda x: tickers.get(x).recommendations_summary.iloc[0]['strongSell'])

    return data, tickers

def get_metrics(tickers: dict, metrics: list) -> pd.DataFrame:
    import yfinance as yf
    data = []
    for symbol, ticker in tickers.items():
        temp = []
        temp.append(symbol)
        for metric in metrics:
            temp.append(ticker.info[metric])
        data.append(temp)
    return pd.DataFrame(data, columns=['symbol', *metrics])

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

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        submissions = response.json()

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

def get_fact(company_facts: dict, fact: str, annual_form: str = "10-K", quarter_form: str = "10-Q") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extracts and organizes quarterly and annual revenue data from a company's financial facts.

    Parameters:
        company_facts (dict): A dictionary containing financial data for a company, structured
                              as per the SEC EDGAR API's `companyfacts` endpoint.
    """
    company_facts_df = None
    type_of_fact = list(company_facts['facts']['us-gaap'][fact]['units'].keys())[0]
    company_facts_df = pd.DataFrame(company_facts['facts']['us-gaap'][fact]['units'][type_of_fact])
    per_share = False
    if type_of_fact == 'USD/shares':
        per_share = True
    
    company_facts_df['start'] = pd.to_datetime(company_facts_df['start'])
    company_facts_df['end'] = pd.to_datetime(company_facts_df['end'])
    company_facts_df['diff'] = company_facts_df['end'] - company_facts_df['start']

    annual_facts = company_facts_df[company_facts_df['form'] == annual_form]
    annual_facts = annual_facts.sort_values(by=['end', 'filed'], ascending=[True, False])

    quarter_facts = company_facts_df[company_facts_df['form'] == quarter_form]
    quarter_facts = quarter_facts.sort_values(by=['end', 'filed'], ascending=[True, False])

    annual_data = {
        'Date': [],
        fact: []
    }
    quarter_data = {
        'Date': [],
        fact: []
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
            annual_data[fact].append(row['val'])
            annual_obtained.add(row['end'])
            year_idx[row['end'].year] = counter
            counter += 1


    for _, row in quarter_facts.iterrows():
        if row['end'] in quarter_obtained:
            continue
        elif row['diff'].days >= 89 and row['diff'].days <= 91:
            quarter_data['Date'].append(row['end'])
            quarter_data[fact].append(row['val'])
            quarter_obtained.add(row['end'])


    annual_data = pd.DataFrame(annual_data)
    annual_data['Date'] = pd.to_datetime(annual_data['Date'])
    annual_data = annual_data.set_index('Date')

    quarter_data = pd.DataFrame(quarter_data, columns=['Date', fact])
    quarter_data['Date'] = pd.to_datetime(quarter_data['Date'])
    quarter_data = quarter_data.set_index('Date')

    if len(quarter_data) > 0:
        sum, counter = 0, 0
        prev = quarter_data.index[0].year
        for row in quarter_data.index:
            if row.year == prev:
                counter += 1
                sum += quarter_data.loc[row][fact]
            else:
                prev = row.year
                sum, counter = quarter_data.loc[row][fact], 1
            if counter == 3:
                if len(annual_data[annual_data.index >= row]) > 0:
                    value = annual_data[annual_data.index >= row].iloc[0][fact]
                    time = annual_data[annual_data.index >= row].iloc[0].name
                    quarter_data.loc[time] = value - sum
                sum, counter = 0, 0

        quarter_data = quarter_data.sort_index()
    
    if not per_share:
        quarter_data[fact] = quarter_data[fact].apply(lambda x: f"{x:.0f}")
        annual_data[fact] = annual_data[fact].apply(lambda x: f"{x:.0f}")

    return (quarter_data, annual_data)

def get_special_fact(company_facts: dict, fact: str, annual_form: str = "10-K", quarter_form: str = "10-Q" ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    company_facts_df = None
    type_of_fact = list(company_facts['facts']['us-gaap'][fact]['units'].keys())[0]
    company_facts_df = pd.DataFrame(company_facts['facts']['us-gaap'][fact]['units'][type_of_fact])
    per_share = False
    if type_of_fact == 'USD/shares':
        per_share = True

    company_facts_df['end'] = pd.to_datetime(company_facts_df['end'])

    annual_facts = company_facts_df[company_facts_df['form'] == annual_form]
    annual_facts = annual_facts.sort_values(by=['end', 'filed'], ascending=[True, False])

    quarter_facts = company_facts_df[company_facts_df['form'] == quarter_form]
    quarter_facts = quarter_facts.sort_values(by=['end', 'filed'], ascending=[True, False])

    annual_data = {
        'Date': [],
        fact: []
    }
    quarter_data = {
        'Date': [],
        fact: []
    }
    year_idx = {}

    annual_obtained = set()
    quarter_obtained = set()
    counter = 0
    for _, row in annual_facts.iterrows():
        if row['end'] in annual_obtained:
            continue
        annual_data['Date'].append(row['end'])
        annual_data[fact].append(row['val'])
        annual_obtained.add(row['end'])
        year_idx[row['end'].year] = counter
        counter += 1


    for _, row in quarter_facts.iterrows():
        if row['end'] in quarter_obtained:
            continue
        quarter_data['Date'].append(row['end'])
        quarter_data[fact].append(row['val'])
        quarter_obtained.add(row['end'])


    annual_data = pd.DataFrame(annual_data)
    annual_data['Date'] = pd.to_datetime(annual_data['Date'])
    annual_data = annual_data.set_index('Date')

    quarter_data = pd.DataFrame(quarter_data, columns=['Date', fact])
    quarter_data['Date'] = pd.to_datetime(quarter_data['Date'])
    quarter_data = quarter_data.set_index('Date')

    if len(quarter_data) > 0:
        sum, counter = 0, 0
        prev = quarter_data.index[0].year
        for row in quarter_data.index:
            if row.year == prev:
                counter += 1
                sum += quarter_data.loc[row][fact]
            else:
                prev = row.year
                sum, counter = quarter_data.loc[row][fact], 1
            if counter == 3:
                if len(annual_data[annual_data.index >= row]) > 0:
                    value = annual_data[annual_data.index >= row].iloc[0][fact]
                    time = annual_data[annual_data.index >= row].iloc[0].name
                    quarter_data.loc[time] = value - sum
                sum, counter = 0, 0

        quarter_data = quarter_data.sort_index()
    
    if not per_share:
        quarter_data[fact] = quarter_data[fact].apply(lambda x: f"{x:.0f}")
        annual_data[fact] = annual_data[fact].apply(lambda x: f"{x:.0f}")

    return (quarter_data, annual_data)

def get_recent_fact_from_symbol(symbol: str, fact: str, annual: bool = True, annual_form: str = "10-K", quarter_form: str = "10-Q" ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retrieves most reacet quarter or annual data for a specific financial fact based on a stock symbol.
    """
    cik = get_cik_from_symbol(symbol)
    company_facts = get_company_facts(cik, symbol)
    try:
        quarter_data, annual_data = get_fact(company_facts, fact, annual_form, quarter_form)
    except Exception as e:
        quarter_data, annual_data = get_special_fact(company_facts, fact, annual_form, quarter_form)
    if annual:
        return annual_data[fact].iloc[-1]
    else:
        return quarter_data[fact].iloc[-1]

def get_all_facts(company_facts: dict) -> Tuple[pd.DataFrame, pd.DataFrame, list]:
    """
    Retrieves all available financial facts for a company, organizing them into quarterly and annual data.

    Parameters:
    ----------
    company_facts : dict
        A dictionary containing the financial data for a company.

    Returns:
    -------
    Tuple[pd.DataFrame, pd.DataFrame, list]
        - quarter_data: A DataFrame of all quarterly financial data.
        - annual_data: A DataFrame of all annual financial data.
        - specialCase: A list of facts requiring special handling, including error messages.
    """
    quarter_data, annual_data, specialCase= [], [], []
    for fact in company_facts['facts']['us-gaap'].keys():
        try:
            q, a = get_fact(company_facts, fact)
        except Exception as e:
            specialCase.append((fact, str(e)))
            q, a = get_special_fact(company_facts, fact)
        quarter_data.append(q)
        annual_data.append(a)
    quarter_data = pd.concat(quarter_data, axis=1)
    annual_data = pd.concat(annual_data, axis=1)
    return quarter_data, annual_data, specialCase

def get_description(company_facts: dict, fact: str) -> str:
    """
    Retrieves the description of a specific financial fact.

    Parameters:
    ----------
    company_facts : dict
        A dictionary containing the financial data for a company.
    fact : str
        The financial fact whose description is to be retrieved.

    Returns:
    -------
    str
        A description of the specified financial fact.
    """
    return company_facts['facts']['us-gaap'][fact]['description']

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

def get_indicators(tickers: dict, attribute: str, indicators: list = None) -> pd.DataFrame:
    """
    Extracts and organizes cash flow data for multiple stock tickers into a pandas DataFrame.

    This function processes cash flow data for various indicators, aligning them for all provided tickers. 
    Missing values for some indicators in specific tickers are padded with NaN. Additionally, the function 
    supports filtering to include only specified indicators.
    """
    import numpy as np

    stock_data = {"symbol": [], "year": []}
    for idx, (symbol, ticker) in enumerate(tickers.items()):
        cash_flow = getattr(ticker, attribute)
        stock_data["year"].append(cash_flow.columns[0])
        cash_flow = cash_flow[cash_flow.columns[0]]
        stock_data["symbol"].append(symbol)
        for indicator, val in cash_flow.items():
            if indicator not in stock_data.keys():
                stock_data[indicator] = [val] if idx == 0 else [np.nan] * idx
                if idx != 0:
                    stock_data[indicator].append(val)
            else:
                if len(stock_data[indicator]) != idx:
                    padding = [np.nan] * (idx - len(stock_data[indicator]))
                    stock_data[indicator] = stock_data[indicator] + [np.nan]
                    stock_data[indicator].append(val)
                else:
                    stock_data[indicator].append(val)

    for indicator in stock_data.keys():
        if len(stock_data[indicator]) != len(tickers.keys()):
            padding = [np.nan] * (len(tickers.keys()) - len(stock_data[indicator]))
            stock_data[indicator] = stock_data[indicator] + padding
    df = pd.DataFrame(stock_data)

    if indicators:
        drops = []
        for column in df.columns:
            if column not in indicators:
                drops.append(column)
        df = df.drop(drops, axis=1)

    return df

def line_and_bar(chart_name: str, values_index: pd.Index, values: pd.Series, history_price_index: pd.Index, history_price_open: pd.Series, percentage: bool = False) -> go.Figure:
    """
    The function generates a dual-axis chart that combines a bar chart and a line chart using Plotly's go.Figure. 
    It is particularly useful for comparing two data series, 
    such as annual revenues (or percentages) and historical stock prices, over the same time period.
    """
    bar_chart = None

    if percentage:
        bar_chart = go.Bar(x=values_index, y=values, name='Bar Data', hovertemplate='%{y:.2f}%<extra></extra>')
    else:
        bar_chart = go.Bar(x=values_index, y=values, name='Bar Data')

    line_chart = go.Scatter(x=history_price_index, y=history_price_open, name='Line Data', mode='lines+markers', yaxis='y2')

    fig = go.Figure(data=[line_chart, bar_chart])

    fig.update_layout(
        title= chart_name,
        xaxis_title="Time",
        yaxis_title="Values",
        yaxis2=dict(
            title="Stock Price",
            overlaying="y",
            side="right"
        ),
        legend_title="Legend",
    )
    return fig

def bar_chart(title: str, df: pd.DataFrame, y: str) -> px.bar:
    """
    The function returns a bar chart with the title for companies.
    It takes title, dataframe, and variable to create the bar
    """
    bar = px.bar(df, x='symbol', y=y, title=title, 
                 color='symbol', labels={'symbol': 'Compnay Symbol', y: 'Value'})

    bar.update_layout(
        legend_title_text="Symbols",
        xaxis_title="Company Symbols",
        yaxis_title="Values"
    )
    
    return bar

def calculate_difference(data: pd.Series) -> pd.DataFrame:
    """
    The function calculates the difference and the percentage change between consecutive values in a pandas Series. 
    It returns the results as a pandas DataFrame with two columns: 'Difference' and 'PercentageChange'.
    """
    difference = pd.DataFrame()
    difference['Difference'] = data.diff()
    difference['PercentageChange'] = difference['Difference'] / data.shift(1) * 100

    return difference

