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
    data['image_url'] = data['image'] if 'image' in data.columns else None
    for col in ['item_condition', 'horsepower',
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
    data['image_url'] = data['image'] if 'image' in data.columns else None
    for col in ['color', 'version', 'horsepower',
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


def transform_autocosmos_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['marca'].notna() | data['modelo'].notna()]

    data['product'] = (data['marca'].fillna('') + ' ' + data['modelo'].fillna('')).str.strip()
    data['vehicle_brand'] = data['marca']
    data['vehicle_line'] = data['modelo']
    data['version'] = data.get('version')
    data['color'] = data.get('color')
    data['fuel_type'] = data.get('combustible')
    data['engine'] = data.get('cilindrada')
    data['transmission'] = data.get('transmision')
    data['horsepower'] = data.get('potencia')
    data['traction_control'] = data.get('traccion')
    data['price'] = pd.to_numeric(data['precio_cop'], errors='coerce').astype('Int64')
    data['year'] = pd.to_numeric(data['año'], errors='coerce').astype('Int64')
    data['years'] = data['year'].copy()
    data['mileage'] = pd.to_numeric(data['km'], errors='coerce').fillna(0).astype(int)
    data['location_city2'] = data['ciudad']
    data['location_city'] = None
    data['image_url'] = data.get('image_url')
    data['sku'] = data['listing_id']
    data['id'] = pd.to_numeric(data['listing_id'], errors='coerce').astype('Int64')

    for col in ['linea', 'description', 'body_type', 'last_plate_digit', 'plate_parity',
                'item_condition', 'steering', 'single_owner', 'negotiable_price',
                'json_ld_extra', 'specs_extra']:
        data[col] = None
    data['num_doors'] = pd.array([pd.NA] * len(data), dtype='Int64')
    data['seating_capacity'] = pd.array([pd.NA] * len(data), dtype='Int64')

    data.drop(columns=['marca', 'modelo', 'año', 'km', 'ciudad', 'precio_cop', 'precio_texto',
                       'listing_id', 'combustible', 'cilindrada', 'potencia', 'alimentacion',
                       'cilindros', 'traccion', 'transmision'], errors='ignore', inplace=True)

    return data


def transform_autonal_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['marca'].notna() | data['modelo'].notna()]

    data['product'] = (data['marca'].fillna('') + ' ' + data['modelo'].fillna('')).str.strip()
    data['vehicle_brand'] = data['marca']
    data['vehicle_line'] = data['modelo']
    data['linea'] = data.get('línea')
    data['version'] = data.get('versión')
    data['color'] = data.get('color')
    data['engine'] = data.get('cilindraje')
    data['price'] = pd.to_numeric(data['precio_cop'], errors='coerce').astype('Int64')
    data['year'] = pd.to_numeric(data['año'], errors='coerce').astype('Int64')
    data['years'] = data['year'].copy()
    data['mileage'] = pd.to_numeric(data['km'], errors='coerce').fillna(0).astype(int)
    data['location_city2'] = data.get('ciudad')
    data['location_city'] = None
    data['image_url'] = data.get('image_url')
    data['id'] = pd.to_numeric(data['listing_id'], errors='coerce').astype('Int64')
    data['sku'] = data['listing_id']

    # placa es solo el último dígito
    data['last_plate_digit'] = pd.to_numeric(data.get('placa'), errors='coerce').astype('Int64')
    data['plate_parity'] = data['last_plate_digit'].apply(
        lambda x: 'Impar' if pd.notna(x) and x % 2 != 0 else ('Par' if pd.notna(x) else None)
    )

    for col in ['description', 'body_type', 'fuel_type', 'transmission', 'item_condition',
                'horsepower', 'traction_control', 'steering', 'single_owner',
                'negotiable_price', 'json_ld_extra', 'specs_extra']:
        data[col] = None
    data['num_doors'] = pd.array([pd.NA] * len(data), dtype='Int64')
    data['seating_capacity'] = pd.array([pd.NA] * len(data), dtype='Int64')

    data.drop(columns=['marca', 'modelo', 'año', 'km', 'ciudad', 'precio_cop', 'precio_texto',
                       'listing_id', 'línea', 'versión', 'placa', 'kilometraje', 'ubicación',
                       'cilindraje'], errors='ignore', inplace=True)

    return data


def transform_elpais_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['marca'].notna() | data['modelo'].notna()]

    data['product'] = (data['marca'].fillna('') + ' ' + data['modelo'].fillna('')).str.strip()
    data['vehicle_brand'] = data['marca']
    data['vehicle_line'] = data['modelo']
    data['linea'] = data.get('complemento modelo')
    data['description'] = data.get('descripcion')
    data['color'] = data.get('color principal')
    data['engine'] = data.get('cilindraje')
    # Preferir 'transmisión' del detail sobre 'transmision' del listing
    data['transmission'] = data.get('transmisión').combine_first(data['transmision']) \
        if 'transmisión' in data.columns else data['transmision']
    data['fuel_type'] = data.get('combustible')
    data['steering'] = data.get('dirección')
    data['location_city'] = data.get('ciudad de ubicación del vehículo')
    data['location_city2'] = data.get('departamento de ubicación del vehículo')
    data['last_plate_digit'] = pd.to_numeric(data.get('placa terminada en'), errors='coerce').astype('Int64')
    data['plate_parity'] = data['last_plate_digit'].apply(
        lambda x: 'Impar' if pd.notna(x) and x % 2 != 0 else ('Par' if pd.notna(x) else None)
    )
    data['num_doors'] = pd.to_numeric(data.get('no. puertas'), errors='coerce').astype('Int64')
    data['seating_capacity'] = pd.to_numeric(data.get('no. de asientos'), errors='coerce').astype('Int64')
    data['price'] = pd.to_numeric(data['precio_cop'], errors='coerce').astype('Int64')
    data['year'] = pd.to_numeric(data['año'], errors='coerce').astype('Int64')
    data['years'] = data['year'].copy()
    data['mileage'] = pd.to_numeric(data['km'], errors='coerce').fillna(0).astype(int)
    data['id'] = pd.to_numeric(data['listing_id'], errors='coerce').astype('Int64')
    data['sku'] = data['listing_id']

    for col in ['image_url', 'version', 'body_type', 'item_condition', 'horsepower',
                'traction_control', 'single_owner', 'negotiable_price',
                'json_ld_extra', 'specs_extra']:
        data[col] = None

    data.drop(columns=['marca', 'modelo', 'año', 'km', 'transmision', 'precio_cop',
                       'precio_texto', 'telefono', 'listing_id', 'complemento modelo',
                       'color principal', 'kilometraje', 'transmisión',
                       'país de ubicación del vehículo', 'departamento de ubicación del vehículo',
                       'ciudad de ubicación del vehículo', 'rango de precio', 'tapicería',
                       'placa terminada en', 'descripcion', 'cilindraje', 'combustible',
                       'dirección', 'no. puertas', 'no. de asientos'],
              errors='ignore', inplace=True)

    return data


# Facebook: most sellers outside Bogotá skip the vehicle form, so brand/line often come from the title.
_FACEBOOK_BRANDS = {
    'mercedes benz': 'Mercedes-Benz', 'mercedes-benz': 'Mercedes-Benz', 'mercedes': 'Mercedes-Benz',
    'land rover': 'Land Rover', 'range rover': 'Land Rover', 'great wall': 'Great Wall',
    'alfa romeo': 'Alfa Romeo', 'volkswagen': 'Volkswagen', 'wolkswagen': 'Volkswagen', 'volswagen': 'Volkswagen',
    'vw': 'Volkswagen', 'chevrolet': 'Chevrolet', 'chevy': 'Chevrolet', 'renault': 'Renault', 'renaul': 'Renault',
    'mazda': 'Mazda', 'kia': 'Kia', 'toyota': 'Toyota', 'nissan': 'Nissan', 'nisan': 'Nissan',
    'hyundai': 'Hyundai', 'ford': 'Ford', 'suzuki': 'Suzuki', 'susuki': 'Suzuki', 'mitsubishi': 'Mitsubishi',
    'honda': 'Honda', 'bmw': 'BMW', 'audi': 'Audi', 'peugeot': 'Peugeot', 'jeep': 'Jeep', 'dodge': 'Dodge',
    'ram': 'RAM', 'fiat': 'Fiat', 'citroen': 'Citroën', 'citroën': 'Citroën', 'subaru': 'Subaru',
    'volvo': 'Volvo', 'skoda': 'Skoda', 'seat': 'SEAT', 'byd': 'BYD', 'chery': 'Chery', 'jac': 'JAC',
    'dfsk': 'DFSK', 'mini': 'MINI', 'porsche': 'Porsche', 'lexus': 'Lexus', 'ssangyong': 'SsangYong',
    'daewoo': 'Daewoo', 'daihatsu': 'Daihatsu', 'isuzu': 'Isuzu', 'foton': 'Foton', 'changan': 'Changan',
    'geely': 'Geely', 'mg': 'MG', 'haval': 'Haval', 'jetour': 'Jetour', 'tesla': 'Tesla', 'opel': 'Opel',
}
# Titles that name only the model ('Spark gt 2011', 'Sandero intens automático')
_FACEBOOK_MODELS = {
    'Chevrolet': ['spark', 'aveo', 'sail', 'onix', 'tracker', 'captiva', 'cruze', 'optra', 'beat', 'joy',
                  'd-max', 'dmax', 'luv', 'n300', 'corsa', 'sonic', 'equinox', 'trailblazer', 'colorado', 'vitara'],
    'Renault': ['sandero', 'logan', 'duster', 'stepway', 'clio', 'kwid', 'twingo', 'symbol', 'koleos',
                'captur', 'oroch', 'megane', 'fluence', 'kangoo', 'arkana'],
    'Mazda': ['cx-5', 'cx5', 'cx-30', 'cx30', 'cx-3', 'cx3', 'cx-50', 'cx-9', 'bt-50', 'bt50', 'allegro'],
    'Kia': ['picanto', 'rio', 'sportage', 'cerato', 'soluto', 'sorento', 'stonic', 'seltos', 'niro', 'carnival'],
    'Toyota': ['hilux', 'fortuner', 'corolla', 'prado', 'yaris', 'rav4', 'rav 4', 'land cruiser', 'tundra', 'fj'],
    'Nissan': ['march', 'versa', 'sentra', 'frontier', 'qashqai', 'kicks', 'tiida', 'x-trail', 'xtrail', 'navara'],
    'Hyundai': ['accent', 'tucson', 'i10', 'i25', 'i35', 'santa fe', 'creta', 'atos', 'getz', 'elantra', 'venue'],
    'Ford': ['fiesta', 'ecosport', 'escape', 'explorer', 'ranger', 'focus', 'f-150', 'f150', 'bronco', 'edge'],
    'Volkswagen': ['gol', 'polo', 'jetta', 'tiguan', 't-cross', 'voyage', 'virtus', 'amarok', 'nivus', 'golf'],
    'Suzuki': ['swift', 'grand vitara', 'alto', 'jimny', 'celerio', 'ertiga', 's-presso', 'baleno'],
    'Fiat': ['cronos', 'mobi', 'palio', 'siena', 'strada', 'pulse', 'fastback'],
    'Mitsubishi': ['montero', 'lancer', 'outlander', 'l200', 'eclipse cross', 'asx'],
    'Honda': ['civic', 'cr-v', 'crv', 'hr-v', 'hrv', 'city', 'fit', 'pilot', 'accord'],
}
_FACEBOOK_MODEL_BRAND = {m: b for b, models in _FACEBOOK_MODELS.items() for m in models}


def _alternation(words):
    """Regex for whole-word matches of ``words``, longest first ('mercedes benz' before 'mercedes')."""
    return re.compile(r'(?<![\w-])(' + '|'.join(re.escape(w) for w in sorted(words, key=len, reverse=True))
                      + r')(?![\w-])', re.IGNORECASE)


_FACEBOOK_BRAND_RE = _alternation(_FACEBOOK_BRANDS)
_FACEBOOK_MODEL_RE = _alternation(_FACEBOOK_MODEL_BRAND)
_YEAR_RE = re.compile(r'(19|20)\d{2}')
_FACEBOOK_FUEL = {'PETROL': 'Gasolina', 'GASOLINE': 'Gasolina', 'DIESEL': 'Diésel', 'HYBRID': 'Híbrido',
                  'PLUGIN_HYBRID': 'Híbrido enchufable', 'ELECTRIC': 'Eléctrico', 'FLEX': 'Flex', 'OTHER': None}
_FACEBOOK_TRANSMISSION = {'AUTOMATIC': 'Automática', 'MANUAL': 'Mecánica'}


def _facebook_clean(text):
    """'2020 Renault+ Logan+' -> '2020 Renault Logan' (some apps post the title URL-encoded)."""
    if not isinstance(text, str):
        return None
    return re.sub(r'\s+', ' ', text.replace('+', ' ')).strip() or None


_KM_RE = re.compile(r'(?<![\d.,])(\d{1,3}(?:[.,]\d{3})+|\d+(?:[.,]\d+)?)\s*(mil)?\s*'
                    r'(?:km|kms|kil[oó]metros|kilometraje)\b', re.IGNORECASE)


def _facebook_km(text):
    """Mileage written in the description: '144.460 kilómetros' -> 144460, '39mil km' -> 39000.
    Small bare numbers ('53km originales', meaning 53 thousand) are ambiguous and skipped."""
    if not isinstance(text, str):
        return None
    for number, thousands in _KM_RE.findall(text):
        if thousands:
            value = float(number.replace(',', '.')) * 1000
        elif re.fullmatch(r'\d{1,3}(?:[.,]\d{3})+', number):
            value = int(re.sub(r'[.,]', '', number))
        else:
            value = float(number.replace(',', '.'))
        if 1000 <= value <= 1_500_000:
            return int(value)
    return None


def _facebook_brand_line(text):
    """Known brand in ``text`` and the word after it, else a known model and its brand:
    'Vendo Renault clio 2011' -> ('Renault', 'clio'); 'Spark gt 2011' -> ('Chevrolet', 'Spark')."""
    text = text or ''
    match = _FACEBOOK_BRAND_RE.search(text)
    if match:
        rest = [w for w in text[match.end():].split() if not _YEAR_RE.fullmatch(w)]
        return _FACEBOOK_BRANDS[match.group(1).lower()], (rest[0] if rest else None)
    match = _FACEBOOK_MODEL_RE.search(text)
    if match:
        return _FACEBOOK_MODEL_BRAND[match.group(1).lower()], match.group(1)
    return None, None


def _facebook_vehicle(row):
    """(brand, line): the seller's form values when they make sense, else what the title says."""
    title_brand, title_line = _facebook_brand_line(row['product'])
    form_brand = _facebook_clean(row['brand'])
    form_line = _facebook_clean(row['line'])
    brand = _facebook_brand_line(form_brand)[0] if form_brand else None   # 'MAZDA 2 GRAND TOURING' -> 'Mazda'
    if form_line and _YEAR_RE.fullmatch(form_line):                     # sellers who type the year as model
        form_line = None
    return brand or title_brand or form_brand, form_line or title_line


def transform_facebook_to_df(json_data):
    data = pd.DataFrame(json_data)
    data = data[data['title'].notna()]

    for col in ['brand', 'line', 'version', 'fuel_type', 'transmission', 'description', 'condition', 'mileage']:
        if col not in data.columns:
            data[col] = None

    data['product'] = data['title'].map(_facebook_clean)
    data['link'] = data['url']
    vehicle = data.apply(_facebook_vehicle, axis=1)
    data['vehicle_brand'] = vehicle.str[0]
    data['vehicle_line'] = vehicle.str[1]
    data['version'] = data['version'].map(_facebook_clean)
    data['fuel_type'] = data['fuel_type'].map(lambda x: _FACEBOOK_FUEL.get(x, x) if isinstance(x, str) else None)
    data['transmission'] = data['transmission'].map(
        lambda x: _FACEBOOK_TRANSMISSION.get(x, x) if isinstance(x, str) else None)
    data['item_condition'] = data['condition']
    data['price'] = pd.to_numeric(data['price'], errors='coerce').astype('Int64')
    data['year'] = pd.to_numeric(data['year'], errors='coerce').astype('Int64')
    data['years'] = data['year'].copy()
    data['mileage'] = pd.to_numeric(data['mileage'], errors='coerce') \
        .combine_first(data['description'].map(_facebook_km)).fillna(0).astype(int)

    # 'Villavicencio, Meta' / 'Bogotá, D.C., Colombia' -> city, department
    parts = data['city'].fillna('').str.replace(r',\s*Colombia$', '', regex=True).str.split(',', n=1)
    data['location_city'] = parts.str[0].str.strip().replace('', None)
    data['location_city2'] = parts.str[1].str.strip()

    data['sku'] = data['ad_id'].astype(str)
    data['id'] = pd.to_numeric(data['ad_id'], errors='coerce').astype('Int64')
    data['specs_extra'] = data.apply(
        lambda r: json.dumps({'published_at': r.get('published_at'), 'search_city': r.get('search_city')}), axis=1)

    for col in ['linea', 'color', 'body_type', 'engine', 'horsepower', 'traction_control', 'steering',
                'last_plate_digit', 'plate_parity', 'single_owner', 'negotiable_price', 'json_ld_extra']:
        data[col] = None
    data['num_doors'] = pd.array([pd.NA] * len(data), dtype='Int64')
    data['seating_capacity'] = pd.array([pd.NA] * len(data), dtype='Int64')

    data.drop(columns=['source', 'url', 'ad_id', 'title', 'brand', 'line', 'condition', 'city',
                       'published_at', 'search_city', 'is_new', '_created'],
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
