import requests
import pandas as pd
import os
import json

def fetch_climate_data():
    """Fetch climate-related data from World Bank API"""
    print("Fetching climate data...")
    
    climate_indicators = {
        'AG.LND.PRCP.MM': 'precipitation',
        'EN.ATM.CO2E.KT': 'co2_emissions',
        'EN.ATM.METH.KT.CE': 'methane_emissions'
    }
    
    all_climate_data = []
    
    for code, name in climate_indicators.items():
        url = f"http://api.worldbank.org/v2/country/all/indicator/{code}?format=json&per_page=1000"
        
        try:
            response = requests.get(url)
            data = response.json()
            
            if len(data) > 1:
                for record in data[1]:
                    if record.get('value') is not None:
                        all_climate_data.append({
                            'country': record['country']['value'],
                            'country_code': record['countryiso3code'],
                            'year': int(record['date']),
                            'indicator': name,
                            'value': record['value']
                        })
                print(f"✓ Fetched {name} data")
                
        except Exception as e:
            print(f"✗ Error fetching {name}: {e}")
    
    return pd.DataFrame(all_climate_data)

def create_agriculture_data():
    """Create simulated agriculture data (in real scenario, this would be from FAO API)"""
    print("Creating agriculture data...")
    
    # Sample countries for analysis
    countries = ['USA', 'CHN', 'IND', 'BRA', 'RUS', 'FRA', 'GER', 'JPN', 'AUS', 'CAN',
                 'MEX', 'IDN', 'TUR', 'ZAF', 'THA', 'GBR', 'ITA', 'ESP', 'ARG', 'NGA']
    
    agriculture_data = []
    
    for country in countries:
        for year in range(1990, 2023):
            # Simulate crop production
            base_production = 10000 + (year - 1990) * 500
            crop_production = base_production + (hash(country) % 5000)
            
            # Simulate food security index (0-100)
            food_security = 50 + (year - 1990) * 1.5 + (hash(country) % 30)
            
            # Simulate cereal yield (kg per hectare)
            cereal_yield = 2000 + (year - 1990) * 50 + (hash(country) % 1000)
            
            agriculture_data.append({
                'country': country,
                'year': year,
                'crop_production_tonnes': crop_production,
                'food_security_index': min(100, food_security),
                'cereal_yield_kg_ha': cereal_yield,
                'agricultural_land_pct': 30 + (hash(country) % 50)
            })
    
    return pd.DataFrame(agriculture_data)

def main():
    """Main data collection function for climate and agriculture analysis"""
    print("=== Climate & Agriculture Data Collection ===")
    
    os.makedirs('/app/data/raw', exist_ok=True)
    
    climate_df = fetch_climate_data()
    
    if climate_df is not None and not climate_df.empty:
        # Save climate data
        climate_df.to_parquet('/app/data/raw/climate_data.parquet', index=False)
        climate_df.to_csv('/app/data/raw/climate_data.csv', index=False)
        print(f"✓ Climate data saved: {len(climate_df)} records")
    
    # Create agriculture data
    agriculture_df = create_agriculture_data()
    agriculture_df.to_parquet('/app/data/raw/agriculture_data.parquet', index=False)
    agriculture_df.to_csv('/app/data/raw/agriculture_data.csv', index=False)
    print(f"✓ Agriculture data saved: {len(agriculture_df)} records")
    
    # Create dataset description
    dataset_info = {
        "research_question": "How do climate factors correlate with agricultural productivity?",
        "datasets": {
            "climate_data": {
                "source": "World Bank API",
                "indicators": ["precipitation", "co2_emissions", "methane_emissions"],
                "records": len(climate_df) if climate_df is not None else 0
            },
            "agriculture_data": {
                "source": "Simulated FAO-style data",
                "indicators": ["crop_production", "food_security", "cereal_yield"],
                "records": len(agriculture_df)
            }
        },
        "time_period": "1990-2022",
        "countries": "Global coverage"
    }
    
    with open('/app/data/raw/dataset_info.json', 'w') as f:
        json.dump(dataset_info, f, indent=2)
    
    print("✓ Dataset information saved")
    print("\n=== Data Collection Complete ===")
    print("Files created in data/raw/:")
    print("- climate_data.csv & .parquet")
    print("- agriculture_data.csv & .parquet") 
    print("- dataset_info.json")

if __name__ == "__main__":
    main()