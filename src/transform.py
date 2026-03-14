import json
import pandas as pd
import re


def transform_json_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['product'].notna()]

    # Extract json_ld fields
    json_ld = data['json_ld'].apply(lambda x: x if isinstance(x, dict) else {})
    data['color'] = json_ld.apply(lambda x: x.get('color'))
    data['body_type'] = json_ld.apply(lambda x: x.get('bodyType'))
    data['fuel_type'] = json_ld.apply(lambda x: x.get('fuelType'))
    data['num_doors'] = json_ld.apply(lambda x: x.get('numberOfDoors'))
    data['engine'] = json_ld.apply(lambda x: x.get('vehicleEngine'))
    data['transmission'] = json_ld.apply(lambda x: x.get('vehicleTransmission'))
    data['image_url'] = json_ld.apply(lambda x: x.get('image'))
    data['sku'] = json_ld.apply(lambda x: x.get('sku'))
    data['item_condition'] = json_ld.apply(lambda x: x.get('itemCondition'))

    # Extract specs fields
    specs = data['specs'].apply(lambda x: x if isinstance(x, dict) else {})
    data['year'] = specs.apply(lambda x: x.get('Año'))
    data['version'] = specs.apply(lambda x: x.get('Versión'))
    data['horsepower'] = specs.apply(lambda x: x.get('Potencia'))
    data['seating_capacity'] = specs.apply(lambda x: x.get('Capacidad de personas'))
    data['traction_control'] = specs.apply(lambda x: x.get('Control de tracción'))
    data['steering'] = specs.apply(lambda x: x.get('Dirección'))
    data['last_plate_digit'] = specs.apply(lambda x: x.get('Último dígito de la placa'))
    data['plate_parity'] = specs.apply(lambda x: x.get('Paridad de la placa'))
    data['single_owner'] = specs.apply(lambda x: x.get('Único dueño'))
    data['negotiable_price'] = specs.apply(lambda x: x.get('Con precio negociable'))

    # Derived fields: vehicle_brand from specs.Marca, vehicle_line from specs.Modelo
    data['vehicle_brand'] = specs.apply(lambda x: x.get('Marca')).fillna(
        data['product'].str.split(' ').str[0].str.strip()
    )
    data['vehicle_line'] = specs.apply(lambda x: x.get('Modelo')).fillna(
        data['product'].str.split(' ').str[1].str.strip()
    )
    data['id'] = data['link'].apply(extract_pub_number_from_link)
    data['mileage'] = clean_mileage(data.pop('kilometraje'))

    # Enforce consistent numeric types
    data['price'] = pd.to_numeric(data['price'], errors='coerce').astype('Int64')
    data['years'] = pd.to_numeric(data['years'], errors='coerce').astype('Int64')
    data['year'] = pd.to_numeric(data['year'], errors='coerce').astype('Int64')
    data['num_doors'] = pd.to_numeric(data['num_doors'], errors='coerce').astype('Int64')
    data['seating_capacity'] = pd.to_numeric(data['seating_capacity'], errors='coerce').astype('Int64')
    data['last_plate_digit'] = pd.to_numeric(data['last_plate_digit'], errors='coerce').astype('Int64')
    data['id'] = pd.to_numeric(data['id'], errors='coerce').astype('Int64')
    data['engine'] = data['engine'].astype(str).where(data['engine'].notna(), None)
    data = clean_locations(data)

    # Store remaining non-extracted keys from dicts
    _json_ld_extracted = {
        'color', 'bodyType', 'fuelType', 'numberOfDoors', 'vehicleEngine',
        'vehicleTransmission', 'image', 'sku', 'itemCondition', 'brand',
    }
    _specs_extracted = {
        'Marca', 'Modelo', 'Año', 'Versión', 'Potencia',
        'Capacidad de personas', 'Control de tracción', 'Dirección',
        'Último dígito de la placa', 'Paridad de la placa',
        'Único dueño', 'Con precio negociable',
    }
    data['json_ld_extra'] = json_ld.apply(
        lambda x: json.dumps({k: v for k, v in x.items() if k not in _json_ld_extracted}) or None
    )
    data['specs_extra'] = specs.apply(
        lambda x: json.dumps({k: v for k, v in x.items() if k not in _specs_extracted}) or None
    )

    # Drop raw nested columns
    data.drop(columns=['json_ld', 'specs'], inplace=True)

    return data


def extract_pub_number_from_link(url):
    match = re.search(r'MCO-(\d+)', url)
    if match:
        return match.group(1)
    return None


def clean_mileage(col: pd.Series):
    return col.str.replace('.', '', regex=False).str.split(' ').str[0].astype(int)


def clean_locations(df):
    df['location_city2'] = df['locations'].str.split('-').str[0].str.strip()
    df['location_city'] = df['locations'].str.split('-').str[1].str.strip()
    df.drop(columns='locations', inplace=True)
    return df
