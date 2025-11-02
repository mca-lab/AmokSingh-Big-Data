import requests
import pandas as pd
import os
import sys

def fetch_world_bank_data(indicator_code, indicator_name):
    """Simple data fetcher without complex dependencies"""
    print(f"Fetching {indicator_name}...")
    
    url = f"http://api.worldbank.org/v2/country/all/indicator/{indicator_code}?format=json&per_page=500"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"HTTP Error: {response.status_code}")
            return None
            
        data = response.json()
        
        if len(data) < 2:
            print("No data received")
            return None
            
        records = []
        for item in data[1]:
            if item.get('value') is not None:
                records.append({
                    'country': item['country']['value'],
                    'country_code': item['countryiso3code'],
                    'year': item['date'],
                    'value': item['value']
                })
        
        print(f"✓ {indicator_name}: {len(records)} records")
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"✗ Error fetching {indicator_name}: {e}")
        return None

def main():
    print("=== World Bank Data Collection ===")
    
    # Set up paths
    base_dir = '/app/data'
    raw_dir = f'{base_dir}/raw'
    
    # Create directories
    os.makedirs(raw_dir, exist_ok=True)
    print(f"Working directory: {raw_dir}")
    
    # Indicators to fetch
    indicators = {
        'NY.GDP.PCAP.CD': 'gdp_per_capita',
        'SP.DYN.LE00.IN': 'life_expectancy'
    }
    
    success_count = 0
    
    for code, name in indicators.items():
        df = fetch_world_bank_data(code, name)
        
        if df is not None and not df.empty:
            # Save as CSV (primary format)
            csv_file = f'{raw_dir}/{name}.csv'
            df.to_csv(csv_file, index=False)
            
            # Try to save as Parquet (if pyarrow works)
            try:
                parquet_file = f'{raw_dir}/{name}.parquet'
                df.to_parquet(parquet_file, index=False)
                print(f"✓ Saved {name}.parquet")
            except Exception as e:
                print(f"Note: Could not save Parquet for {name}: {e}")
            
            print(f"✓ Saved {name}.csv with {len(df)} records")
            success_count += 1
    
    # Final verification
    print(f"\n=== Collection Complete ===")
    print(f"Successfully fetched: {success_count}/{len(indicators)} datasets")
    
    if success_count > 0:
        print("Files created:")
        for file in os.listdir(raw_dir):
            file_path = os.path.join(raw_dir, file)
            size = os.path.getsize(file_path)
            print(f"  - {file} ({size} bytes)")
    else:
        print("No data was fetched successfully")

if __name__ == "__main__":
    main()