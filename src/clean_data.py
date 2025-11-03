import pandas as pd
import os

def main():
    print("Starting Data Cleaning with Pandas...")
    
    # Create processed directory
    os.makedirs('/app/data/processed', exist_ok=True)
    
    # Load raw data
    climate_df = pd.read_csv('/app/data/raw/climate_data.csv')
    agriculture_df = pd.read_csv('/app/data/raw/agriculture_data.csv')
    
    print(f"Raw data loaded: {len(climate_df)} climate records, {len(agriculture_df)} agriculture records")
    
    # Simple cleaning - remove duplicates and nulls
    climate_clean = climate_df.drop_duplicates().dropna(subset=['country'])
    agriculture_clean = agriculture_df.drop_duplicates().dropna(subset=['country'])
    
    # Transform climate data to wide format
    climate_wide = climate_clean.pivot_table(
        index=['country', 'year'],
        columns='indicator',
        values='value',
        aggfunc='first'
    ).reset_index()
    
    climate_wide.columns.name = None  # Remove column name
    
    # Join datasets
    final_data = pd.merge(climate_wide, agriculture_clean, on=['country', 'year'])
    
    # Save cleaned data
    final_data.to_parquet('/app/data/processed/cleaned_data.parquet', index=False)
    final_data.to_csv('/app/data/processed/cleaned_data.csv', index=False)
    
    print(f"Cleaning complete! Final data: {len(final_data)} records")
    print("\nSample of cleaned data:")
    print(final_data.head())
    
    print("\nData Summary:")
    print(f"Countries: {final_data['country'].nunique()}")
    print(f"Years: {final_data['year'].nunique()}")
    print(f"Columns: {len(final_data.columns)}")

if __name__ == "__main__":
    main()