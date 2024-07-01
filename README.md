# azure-stock-analysis-etl-pipeline

## Overview
This ETL (Extract, Transform, Load) pipeline is designed for evaluating the financial health and investment potential of stocks based on fundamental analysis. It automates the collection and transformation of stock data, facilitating access to refined financial metrics and trends to assess investment quality.
Additionally it updates the daily top gainers, loosers and sectorial heatmap of top performing sectors and stocks

![alt text](https://github.com/mihirajgaonkar/azure-stock-analysis-etl-pipeline/blob/main/dashboard%20ss.png)

## Technical Architecture
### Storage Account Setup
- **Containers**: Two Azure containers are established; one for storing raw JSON files and another for CSV files with transformed data.

### Azure Functions
- **Timer Trigger**: Utilizes a Timer Trigger to fetch JSON files of S&P 100 stocks and store them in blob storage. This can alternatively be handled directly through Azure Synapse Analytics.

### Permissions Management
- **Access Control**: Ensures that appropriate permissions (data owner/contributor) are assigned to the containers.

### Analytics Workspace
- **Integration**: An Azure analytics workspace and Spark session are set up with a linked service connection between the containers and the Synapse workspace.

### Data Processing Notebooks
- **Notebooks**: Two notebooks are used:
  - One for parsing JSON files and extracting fundamental financial metrics.
  - Another for calculating the current stock price and its percentage change from the opening price.
  - The transformed data is compiled in CSV format and stored in a designated container.

### Trigger Setup
- **Automation**: Triggers in Synapse Analytics are configured for both notebooks. One updates every 30 minutes, and the other activates when new JSON data is added.

![alt text](tech architecture)

### Data Visualization
- **Power BI Integration**: Connects to the transformed data container using Power BI for advanced analytics and visualization creation.
- **Visualization Demo**: [click here](https://drive.google.com/file/d/1RKv-SI0lG9h2biU72lZQkIT1JNQuqZl5/view?usp=sharing)

![alt text](data model)

##Fundamental Metrics Explained
- **Forward EPS _Earnings per share_**: Indicates expected profitability; higher EPS suggests better profitability.
- **Forward P/E _Price to Earnings ratio_**: Compares a company's current share price to its forward earnings per share. A lower P/E ratio may indicate that the stock is undervalued. Note that each industry has it's own P/E value and stocks from different industries cannot be compared based on their individual P/E value 
- **PEG Ratio _Price/Earnings to growth ratio_**: If PEG is < 1 then it is typically considered a good stock 
- **FCFY _Free cashflow yield_**: Measures the free cash flow relative to the market value of the company. Higher FCFY indicates better cash flow generation relative to the company's value
- **Price to Book (P/B) Ratio _Price to book ratio_**: Compares a company's market value to its book value. A lower P/B ratio can indicate the stock is undervalued, particularly if the company has strong assets
- **Return on Equity (ROE)**: Measures how well it uses shareholders equity to generate profits, High ROE indicates good use of investments
- **Trailing P/S _Price to sales_**: The P/S ratio is useful for comparing companies, especially those with little or no profit or negative cash flow, for example grocery companies generally have P/S of 0.1 or 0.2 because they operate in low profit margins. A lower value suggests an investor is getting more value for their money
- **Dividend Yield Ratios (DY and DPR)**: Indicates the proportion of earnings paid out as dividends, A moderate DPR is often considered sustainable. Very high DPR may indicate potential risks if earnings decline. Government stocks generally have a high dividend yield
- **Current Ratio (CR)**: A company with current ratio > 1.5 is generally considered a good company, Measures a company's ability to pay short-term obligations. A higher CR indicates better liquidity and financial health.
- **Beta**: Reflects stock volatility relative to the overall market.
- **52-Week Range**: Indicates the percentage change of the stock price within the last year relative to its high and low.

##Future improvements:
- Create heat map for sector and stocks for monthly changes as there will be volatility in the coming months due to elections
- Use azure managed identity to provide necessary access and permissions and create specific roles 
- Do not hard code the connect strings and keys of services in the code, use SAS for limited time access or define RBAC policies 

