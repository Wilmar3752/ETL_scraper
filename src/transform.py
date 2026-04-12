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


def transform_carroya_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['product'].notna()]

    json_ld = data['json_ld'].apply(lambda x: x if isinstance(x, dict) else {})
    specs = data['specs'].apply(lambda x: x if isinstance(x, dict) else {})

    # Fields from json_ld
    data['vehicle_brand'] = json_ld.apply(
        lambda x: x['brand']['name'] if isinstance(x.get('brand'), dict) else x.get('brand')
    )
    data['sku'] = json_ld.apply(lambda x: x.get('sku'))
    data['image_url'] = json_ld.apply(lambda x: x.get('image'))

    # vehicle_line: product name minus the brand prefix
    data['vehicle_line'] = data.apply(
        lambda row: row['product'].replace(row['vehicle_brand'], '', 1).strip()
        if pd.notna(row['vehicle_brand']) else row['product'],
        axis=1
    )

    # Fields from specs
    data['item_condition'] = specs.apply(lambda x: x.get('ESTADO'))
    data['transmission'] = specs.apply(lambda x: x.get('TIPO DE CAJA'))
    data['fuel_type'] = specs.apply(lambda x: x.get('COMBUSTIBLE'))
    data['engine'] = specs.apply(lambda x: x.get('CILINDRAJE'))
    data['color'] = specs.apply(lambda x: x.get('COLOR'))

    # id from vehicle_id
    data['id'] = pd.to_numeric(data['vehicle_id'], errors='coerce').astype('Int64')

    # year → year and years
    data['year'] = pd.to_numeric(data['year'], errors='coerce').astype('Int64')
    data['years'] = data['year'].copy()

    # price: "$129.990.000" → 129990000
    data['price'] = (
        data['price'].str.replace(r'[\$\.\s]', '', regex=True)
    )
    data['price'] = pd.to_numeric(data['price'], errors='coerce').astype('Int64')

    # mileage: "30.576 Km" → 30576
    data['mileage'] = (
        data['kilometraje'].str.replace('.', '', regex=False).str.split(' ').str[0]
    )
    data['mileage'] = pd.to_numeric(data['mileage'], errors='coerce').fillna(0).astype(int)

    # plate: "Placa **5" → last_plate_digit and plate_parity
    data['last_plate_digit'] = data['plate'].str.extract(r'(\d)$')
    data['last_plate_digit'] = pd.to_numeric(data['last_plate_digit'], errors='coerce').astype('Int64')
    data['plate_parity'] = data['last_plate_digit'].apply(
        lambda x: 'Impar' if pd.notna(x) and x % 2 != 0 else ('Par' if pd.notna(x) else None)
    )

    # location
    data['location_city2'] = data['location']
    data['location_city'] = None

    # Fields not available in Carroya
    for col in ['body_type', 'version', 'horsepower', 'traction_control',
                'steering', 'single_owner', 'negotiable_price', 'description']:
        data[col] = None
    data['num_doors'] = pd.array([pd.NA] * len(data), dtype='Int64')
    data['seating_capacity'] = pd.array([pd.NA] * len(data), dtype='Int64')

    # Extra fields
    _json_ld_extracted = {'brand', 'sku', 'image'}
    _specs_extracted = {'ESTADO', 'TIPO DE CAJA', 'COMBUSTIBLE', 'CILINDRAJE', 'COLOR'}
    data['json_ld_extra'] = json_ld.apply(
        lambda x: json.dumps({k: v for k, v in x.items() if k not in _json_ld_extracted})
    )
    data['specs_extra'] = specs.apply(
        lambda x: json.dumps({k: v for k, v in x.items() if k not in _specs_extracted})
    )

    data.drop(columns=['json_ld', 'specs', 'vehicle_id', 'kilometraje',
                        'location', 'plate', 'seller_name', 'seller_address'],
              errors='ignore', inplace=True)

    return data


def transform_usados_renting_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['product'].notna()]

    specs = data['specs'].apply(lambda x: x if isinstance(x, dict) else {})

    # Fields from specs
    data['vehicle_brand'] = specs.apply(lambda x: x.get('Marca'))
    data['vehicle_line'] = data.apply(
        lambda row: row['product'].replace(row['vehicle_brand'], '', 1).strip()
        if pd.notna(row['vehicle_brand']) else row['product'],
        axis=1
    )
    data['color'] = specs.apply(lambda x: x.get('Color'))
    data['body_type'] = specs.apply(lambda x: x.get('Tipo de vehículo'))
    data['fuel_type'] = specs.apply(lambda x: x.get('Combustible'))
    data['engine'] = specs.apply(lambda x: x.get('Motor'))
    data['transmission'] = specs.apply(lambda x: x.get('Transmisión'))
    data['version'] = specs.apply(lambda x: x.get('Modelo'))

    # id: extract numeric suffix from vehicle_id slug (e.g. "honda-jazz-2007-23423" → 23423)
    data['id'] = data['vehicle_id'].str.extract(r'(\d+)$')
    data['id'] = pd.to_numeric(data['id'], errors='coerce').astype('Int64')

    # year
    data['year'] = pd.to_numeric(data['year'], errors='coerce').astype('Int64')
    data['years'] = data['year'].copy()

    # price: already cleaned int by scraper
    data['price'] = pd.to_numeric(data['price'], errors='coerce').astype('Int64')

    # mileage: already cleaned int by scraper
    data['mileage'] = pd.to_numeric(data['kilometraje'], errors='coerce').fillna(0).astype(int)

    # plate: try listing-level plate first, then specs
    plate_col = data['plate'].combine_first(specs.apply(lambda x: x.get('Placa')))
    data['last_plate_digit'] = plate_col.str.extract(r'(\d)$')
    data['last_plate_digit'] = pd.to_numeric(data['last_plate_digit'], errors='coerce').astype('Int64')
    data['plate_parity'] = data['last_plate_digit'].apply(
        lambda x: 'Impar' if pd.notna(x) and x % 2 != 0 else ('Par' if pd.notna(x) else None)
    )

    # location
    data['location_city2'] = data['location'].combine_first(specs.apply(lambda x: x.get('Ubicación')))
    data['location_city'] = specs.apply(lambda x: x.get('Ciudad matrícula'))

    # description already at top level from detail scrape
    if 'description' not in data.columns:
        data['description'] = None

    # sku: vehicle_id is the unique listing identifier (e.g. "VCM005" — plate number)
    data['sku'] = data['vehicle_id']

    # Fields not available in usados-renting
    for col in ['image_url', 'item_condition', 'horsepower',
                'traction_control', 'steering', 'single_owner', 'negotiable_price']:
        data[col] = None
    data['num_doors'] = pd.array([pd.NA] * len(data), dtype='Int64')
    data['seating_capacity'] = pd.array([pd.NA] * len(data), dtype='Int64')

    # Extra specs
    _specs_extracted = {
        'Marca', 'Color', 'Tipo de vehículo', 'Combustible', 'Motor',
        'Transmisión', 'Modelo', 'Placa', 'Descripción', 'Ubicación', 'Ciudad matrícula',
    }
    data['json_ld_extra'] = None
    data['specs_extra'] = specs.apply(
        lambda x: json.dumps({k: v for k, v in x.items() if k not in _specs_extracted})
    )

    data.drop(columns=['vehicle_id', 'kilometraje', 'location', 'plate', 'specs', '_created'],
              errors='ignore', inplace=True)

    return data


def transform_vendetunave_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['product'].notna()]

    specs = data['specs'].apply(lambda x: x if isinstance(x, dict) else {})

    # Fields from specs
    data['vehicle_brand'] = specs.apply(lambda x: x.get('Marca'))
    data['vehicle_line'] = data.apply(
        lambda row: row['product'].replace(row['vehicle_brand'], '', 1).strip()
        if pd.notna(row['vehicle_brand']) else row['product'],
        axis=1
    )
    data['body_type'] = specs.apply(lambda x: x.get('Tipo'))
    data['fuel_type'] = specs.apply(lambda x: x.get('Combustible'))
    data['engine'] = specs.apply(lambda x: x.get('Cilindraje'))
    data['transmission'] = specs.apply(lambda x: x.get('Transmisión'))
    data['item_condition'] = specs.apply(lambda x: x.get('Condición'))

    # id and sku: vehicle_id is a numeric string
    data['id'] = pd.to_numeric(data['vehicle_id'], errors='coerce').astype('Int64')
    data['sku'] = data['vehicle_id']

    # year
    data['year'] = pd.to_numeric(data['year'], errors='coerce').astype('Int64')
    data['years'] = data['year'].copy()

    # price
    data['price'] = pd.to_numeric(data['price'], errors='coerce').astype('Int64')

    # mileage
    data['mileage'] = pd.to_numeric(data['kilometraje'], errors='coerce').fillna(0).astype(int)

    # plate: already just the last digit
    data['last_plate_digit'] = pd.to_numeric(data['plate'], errors='coerce').astype('Int64')
    data['plate_parity'] = data['last_plate_digit'].apply(
        lambda x: 'Impar' if pd.notna(x) and x % 2 != 0 else ('Par' if pd.notna(x) else None)
    )

    # location
    data['location_city2'] = data['location']
    data['location_city'] = None

    # Fields not available
    for col in ['color', 'version', 'image_url', 'horsepower',
                'traction_control', 'steering', 'single_owner', 'negotiable_price']:
        data[col] = None
    data['num_doors'] = pd.array([pd.NA] * len(data), dtype='Int64')
    data['seating_capacity'] = pd.array([pd.NA] * len(data), dtype='Int64')
    data['linea'] = None

    # Extra specs
    _specs_extracted = {'Marca', 'Tipo', 'Combustible', 'Cilindraje', 'Transmisión', 'Condición', 'Modelo'}
    data['json_ld_extra'] = None
    data['specs_extra'] = specs.apply(
        lambda x: json.dumps({k: v for k, v in x.items() if k not in _specs_extracted})
    )

    data.drop(columns=['vehicle_id', 'kilometraje', 'location', 'plate', 'specs', '_created'],
              errors='ignore', inplace=True)

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
