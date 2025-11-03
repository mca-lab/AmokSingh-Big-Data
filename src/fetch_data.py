import pandas as pd
import os

def main():
    print("Starting Climate & Agriculture Data Collection...")
    
    # Create data directory
    os.makedirs('/app/data/raw', exist_ok=True)
    
    # 1. Create Sample Climate Data
    print("Creating climate data...")
    
    climate_data = []
    countries = ['USA', 'India', 'China', 'Brazil', 'Germany', 'France', 'Japan', 'Australia',
                 'Canada', 'Mexico', 'UK', 'Italy', 'Spain', 'Russia', 'South Africa']
    
    for country in countries:
        for year in range(2000, 2023):
            # Simulate realistic climate data
            base_temp = 15 + (hash(country) % 20)  # Different base temperatures per country
            base_precip = 500 + (hash(country) % 1000)  # Different base precipitation
            
            climate_data.extend([
                {
                    'country': country,
                    'year': year,
                    'indicator': 'temperature',
                    'value': round(base_temp + (year - 2000) * 0.1, 1),  # Gradual warming
                    'unit': 'Celsius'
                },
                {
                    'country': country,
                    'year': year,
                    'indicator': 'precipitation',
                    'value': base_precip + (year - 2000) * 2,  # Slight changes over time
                    'unit': 'mm'
                },
                {
                    'country': country,
                    'year': year,
                    'indicator': 'co2_emissions',
                    'value': 5000 + (hash(country) % 15000) + (year - 2000) * 100,  # Increasing emissions
                    'unit': 'kt'
                }
            ])
    
    # Save climate data
    climate_df = pd.DataFrame(climate_data)
    climate_df.to_csv('/app/data/raw/climate_data.csv', index=False)
    print(f"✓ Created climate data: {len(climate_df)} records")
    
    # 2. Create Agriculture Data
    print("Creating agriculture data...")
    
    ag_data = []
    
    for country in countries:
        for year in range(2000, 2023):
            # Simulate agriculture data that correlates with climate
            base_production = 10000 + (hash(country) % 50000)
            base_yield = 2000 + (hash(country) % 3000)
            
            ag_data.append({
                'country': country,
                'year': year,
                'crop_production_tonnes': base_production + (year - 2000) * 500,
                'cereal_yield_kg_ha': base_yield + (year - 2000) * 25,
                'food_security_index': min(100, 50 + (year - 2000) * 1.5 + (hash(country) % 30)),
                'arable_land_pct': 20 + (hash(country) % 60)
            })
    
    # Save agriculture data
    ag_df = pd.DataFrame(ag_data)
    ag_df.to_csv('/app/data/raw/agriculture_data.csv', index=False)
    print(f"✓ Created agriculture data: {len(ag_df)} records")
    
    # 3. Create dataset info
    dataset_info = {
        "research_question": "How do climate factors correlate with agricultural productivity?",
        "time_period": "2000-2022",
        "countries": len(countries),
        "climate_indicators": ["temperature", "precipitation", "co2_emissions"],
        "agriculture_indicators": ["crop_production_tonnes", "cereal_yield_kg_ha", "food_security_index", "arable_land_pct"],
        "total_records": {
            "climate": len(climate_df),
            "agriculture": len(ag_df)
        }
    }
    
    print(f"\n=== Data Collection Complete ===")
    print(f"Countries: {len(countries)}")
    print(f"Time period: 2000-2022")
    print(f"Climate records: {len(climate_df)}")
    print(f"Agriculture records: {len(ag_df)}")
    print(f"Files created in data/raw/:")
    print(f"- climate_data.csv")
    print(f"- agriculture_data.csv")

if __name__ == "__main__":
    main()