import pandas as pd

state_name_mapping = {
    'Odisha': 'Odisha',
    'Bihar': 'Bihar',
    'Maharashtra': 'Maharashtra',
    'Chhattisgarh': 'Chhattisgarh',
    'Chandigarh': 'Chandigarh',
    'Delhi': 'NCT of Delhi',
    'Rajasthan': 'Rajasthan',
    'Uttar_Pradesh': 'Uttar Pradesh',
    'Gujarat': 'Gujarat',
    'Punjab': 'Punjab',
    'Kerala': 'Kerala',
    'TamilNadu': 'Tamil Nadu',
    'Telangana': 'Telangana',
    'Himachal Pradesh': 'Himachal Pradesh',
    'Karnataka': 'Karnataka',
    'Jammu_and_Kashmir': 'Jammu & Kashmir',
    'Puducherry': 'Puducherry',
    'Madhya Pradesh': 'Madhya Pradesh',
    'Nagaland': 'Nagaland',
    'Andhra_Pradesh': 'Andhra Pradesh',
    'Jharkhand': 'Jharkhand',
    'Assam': 'Assam',
    'Tripura': 'Tripura',
    'Manipur': 'Manipur',
    'West_Bengal': 'West Bengal',
    'Uttarakhand': 'Uttarakhand',
    'Sikkim': 'Sikkim',
    'Haryana': 'Haryana',
    'Mizoram': 'Mizoram',
    'Meghalaya': 'Meghalaya',
    'Arunachal_Pradesh': 'Arunachal Pradesh',
}

def map_state_names(df: pd.DataFrame, state_col: str = "state", target_col: str = "state_for_map") -> pd.DataFrame:
    df[target_col] = df[state_col].map(state_name_mapping)
    return df