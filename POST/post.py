import requests
import json
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def create_retry_session():
    """Create a requests session with improved retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # Increase total retries
        backoff_factor=2,  # Increase backoff factor
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PATCH"]  # Allow retries on all methods we use
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# API Configuration
VITEC_URL = "https://connect.maklare.vitec.net/Estate/GetEstateList"
VITEC_HEADERS = {
    "Authorization": "Basic NzEyOk42MlpGUXZmeGxSb243TEpINlg3SGI3ck42eThmeTlQdERzdEw5VWNzVWczdEQ5ZlVOc1lhR0FreThBOFFtMHk=",
    "Content-Type": "application/json"
}

WEBFLOW_HEADERS = {
    "Authorization": "Bearer 9e84bfa8cecdb32f70624cb3d51d73dd990c566cfa4d47c24421b07791b37c94",
    "Content-Type": "application/json",
    "accept-version": "2.0.0"
}

VITEC_DATA_PAYLOAD = {
    "customerId": "M13699",
    "onlyFutureViewings": False
}

# Collection IDs
HOUSES_COLLECTION_ID = "6702dfeb3a0e8b27b71945ec"
COOPERATIVES_COLLECTION_ID = "66fedc9b75df0b889d074728"

def make_webflow_request(session, method, url, json=None, params=None, max_retries=5):
    """Make a request to Webflow API with improved error handling."""
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} - Making {method} request to {url}")
            
            # Add rate limiting
            time.sleep(2)  # Increase delay between requests
            
            # Add query parameters for GET requests
            if method == 'GET' and params:
                query_params = '&'.join(f"{k}={v}" for k, v in params.items())
                url = f"{url}?{query_params}"
                print(f"Full URL with params: {url}")
            
            # Make the request
            response = session.request(
                method=method,
                url=url,
                headers=WEBFLOW_HEADERS,
                json=json,
                timeout=30  # Increase timeout
            )
            
            print(f"Got response status code: {response.status_code}")
            
            if response.status_code == 404:
                print("Resource not found")
                return None
            elif response.status_code == 429:
                wait_time = 60 + (attempt * 30)  # Progressive wait on rate limit
                print(f"Rate limit hit, waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            elif response.status_code != 200:
                print(f"Error response from Webflow: {response.text}")
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    print(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                return None
            
            return response.json()
            
        except requests.exceptions.Timeout:
            print(f"Request timed out after 30 seconds on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            return None
            
        except requests.exceptions.ConnectionError:
            print(f"Connection error occurred on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                wait_time = 10 * (attempt + 1)  # Longer wait for connection errors
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            return None
            
        except Exception as e:
            print(f"Error making request on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            return None
    
    print("All retry attempts exhausted")
    return None


def format_single_viewing(viewing: Dict[str, str]) -> str:
    """Format a single viewing time in Swedish format."""
    try:
        start_time = datetime.strptime(viewing['startTime'], "%Y-%m-%dT%H:%M:%S")
        end_time = datetime.strptime(viewing['endTime'], "%Y-%m-%dT%H:%M:%S")
        
        swedish_days = {
            'Monday': 'Måndag',
            'Tuesday': 'Tisdag',
            'Wednesday': 'Onsdag',
            'Thursday': 'Torsdag',
            'Friday': 'Fredag',
            'Saturday': 'Lördag',
            'Sunday': 'Söndag'
        }
        
        swedish_months = {
            'January': 'januari',
            'February': 'februari',
            'March': 'mars',
            'April': 'april',
            'May': 'maj',
            'June': 'juni',
            'July': 'juli',
            'August': 'augusti',
            'September': 'september',
            'October': 'oktober',
            'November': 'november',
            'December': 'december'
        }
        
        day_name = swedish_days[start_time.strftime('%A')]
        month_name = swedish_months[start_time.strftime('%B')]
        day_num = start_time.strftime('%d').lstrip('0')  # Remove leading zero
        
        return f"{day_name} {day_num} {month_name} {start_time.strftime('%Y %H:%M')} - {end_time.strftime('%H:%M')}"
    except (KeyError, ValueError) as e:
        print(f"Error formatting viewing time: {e}")
        return ""

def format_date(date_str: str) -> str:
    """Format date to YYYY-MM-DD format."""
    try:
        if not date_str or date_str == 'N/A':
            return 'N/A'
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return "N/A"

def sanitize_text(text: str, preserve_linebreaks: bool = False) -> str:
    """
    Clean up text while preserving formatting with HTML breaks for Webflow.
    
    Args:
        text: The text to sanitize
        preserve_linebreaks: If True, preserves formatting with HTML
    """
    if not text:
        return ""
        
    if preserve_linebreaks:
        # Replace Windows-style line breaks with Unix-style
        text = text.replace('\r\n', '\n')
        
        # Clean up multiple consecutive line breaks
        while '\n\n\n' in text:
            text = text.replace('\n\n\n', '\n\n')
            
        # Convert line breaks to HTML breaks
        text = text.replace('\n', '<br />')
        
        # Clean up multiple consecutive HTML breaks
        while '<br /><br /><br />' in text:
            text = text.replace('<br /><br /><br />', '<br /><br />')
            
        # Clean up extra spaces
        text = ' '.join(segment.strip() for segment in text.split(' ') if segment.strip())
        
        return text.strip()
    else:
        # Original behavior for non-rich text fields
        return text.replace('\n', ' ').replace('\r', '').replace('  ', ' ').strip()
        
      
def fetch_webflow_collection_schema(collection_id: str) -> Optional[Dict[str, Any]]:
    """Fetch and validate the Webflow collection schema."""
    try:
        collection_url = f"https://api.webflow.com/v2/collections/{collection_id}"
        response = requests.get(collection_url, headers=WEBFLOW_HEADERS)
        response.raise_for_status()
        schema = response.json()
        print(f"Webflow Collection Schema: {json.dumps(schema, indent=2)}")
        return schema
    except requests.exceptions.RequestException as e:
        print(f"Error fetching collection schema: {e}")
        return None

def manual_post_estate(estate_id: str, customer_id: str, collection_id: str) -> None:
    """Manually post a specific estate by its estateId to Webflow."""
    try:
        print(f"Attempting to post estate {estate_id} manually...")
        
        estate_data, association_id = fetch_housing_cooperative(customer_id, estate_id)
        if not estate_data:
            print(f"Failed to fetch data for estate ID {estate_id}")
            return
        
        association_data = fetch_association_details(customer_id, association_id) if association_id else {}
        
        if update_webflow_item(estate_data, association_data or {}, collection_id):
            print(f"Successfully updated estate {estate_id} in Webflow.")
        elif create_webflow_item(estate_data, association_data or {}, collection_id):
            print(f"Successfully created a new item for estate {estate_id} in Webflow.")
        else:
            print(f"Failed to sync estate {estate_id} to Webflow.")
            
    except Exception as e:
        print(f"Error during manual post of estate {estate_id}: {e}")

def main_manual_trigger():
    """Entry point for manually syncing an estate by its ID."""
    print("Manual Estate Posting to Webflow")
    estate_id = input("Enter the estate ID to post: ").strip()
    if not estate_id:
        print("Invalid estate ID. Please try again.")
        return
    
    customer_id = VITEC_DATA_PAYLOAD["customerId"]
    collection_id = COOPERATIVES_COLLECTION_ID
    
    manual_post_estate(estate_id, customer_id, collection_id)



def find_highest_bid(bids: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find the highest bid from a list of bids."""
    try:
        if not bids or not isinstance(bids, list):
            return None

        valid_bids = [bid for bid in bids if isinstance(bid, dict) and 'amount' in bid]
        if not valid_bids:
            return None

        highest_bid = max(valid_bids, key=lambda x: float(x['amount']) if x.get('amount') is not None else 0)
        return highest_bid if highest_bid.get('amount') else None

    except Exception as e:
        print(f"Error finding highest bid: {e}")
        return None


def fetch_estate_list() -> Optional[Dict[str, Any]]:
    """Fetch the estate list from Vitec API."""
    try:
        print("Fetching estate list from Vitec...")
        response = requests.post(
            VITEC_URL, 
            headers=VITEC_HEADERS, 
            data=json.dumps(VITEC_DATA_PAYLOAD)
        )
        response.raise_for_status()
        data = response.json()
        print("Fetched estate list successfully!")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching estate list: {e}")
        return None

def get_value(dictionary, *keys, default_value='N/A'):
    """
    Recursively get a value from a nested dictionary.
    If the key does not exist, return the default value.
    """
    if not dictionary or not keys:
        return default_value
    
    key = keys[0]
    if key in dictionary:
        if len(keys) > 1:
            return get_value(dictionary[key], *keys[1:], default_value=default_value)
        else:
            return dictionary[key]
    else:
        return default_value

def get_main_image_url(images: List[Dict[str, Any]]) -> str:
    """Safely extract the main image URL from images data."""
    try:
        if not images or not isinstance(images, list):
            return 'default_image_url'
        
        first_image = images[0]
        if not isinstance(first_image, dict):
            return 'default_image_url'
            
        return first_image.get('url', 'default_image_url')
    except Exception as e:
        print(f"Error getting main image URL: {e}")
        return 'default_image_url'

def format_viewing_times(viewings: List[Dict[str, Any]]) -> List[str]:
    """Safely format viewing times."""
    try:
        result = ['', '', '']
        if not viewings or not isinstance(viewings, list):
            return result
            
        for i in range(min(len(viewings), 3)):
            viewing = viewings[i]
            if isinstance(viewing, dict):
                result[i] = format_single_viewing(viewing)
        return result
    except Exception as e:
        print(f"Error formatting viewing times: {e}")
        return ['', '', '']

def get_details_from_association_data(association_data: Dict[str, Any]) -> Dict[str, Any]:
    """Safely extract association details from association data."""
    if not association_data:
        return {
            'name': 'N/A',
            'corporate_number': 'N/A',
            'parking': 'N/A',
            'tv_and_broadband': 'N/A',
            'renovations': 'N/A',
            'about_association': 'N/A',
            'transfer_fee': f"{str(economy.get('transferFee', '0')):,}".replace(",", " ") + " kr",
            'comment_monthlyfee': '',
            'number_of_apartments': 'N/A',
            'number_of_premises': 'N/A',
            'other': 'N/A'
        }

    descriptions = association_data.get('descriptions', {})
    economy = association_data.get('economy', {})

    return {
        'name': association_data.get('name', 'N/A'),
        'corporate_number': association_data.get('corporateNumber', 'N/A'),
        'parking': sanitize_text(descriptions.get('parking', 'N/A'), preserve_linebreaks=True),
        'tv_and_broadband': sanitize_text(descriptions.get('tvAndBroadband', 'N/A'), preserve_linebreaks=True),
        'renovations': sanitize_text(descriptions.get('renovations', 'N/A'), preserve_linebreaks=True),
        'about_association': sanitize_text(descriptions.get('generalAboutAssociation', 'N/A'), preserve_linebreaks=True),
        'transfer_fee': str(economy.get('transferFee', '0')),
        'comment_monthlyfee': sanitize_text(economy.get('monthlyFeeInformation', ''), preserve_linebreaks=True),
        'number_of_apartments': str(association_data.get('numberOfApartments', 'N/A')),
        'number_of_premises': str(association_data.get('numberOfPremises', 'N/A')),
        'other': sanitize_text(descriptions.get('other', 'N/A'), preserve_linebreaks=True)
    }


def prepare_webflow_data(estate: Dict[str, Any], association_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        print("Starting data preparation...")
        if not estate or not isinstance(estate, dict):
            print("No valid estate data provided")
            return None

        print(f"Processing estate data for ID: {estate.get('estateId')}")

        # Get association details
        assoc_details = get_details_from_association_data(association_data)

        # Base Information
        base_info = estate.get('baseInformation', {})
        object_address = base_info.get('objectAddress', {})
        street_address = object_address.get('streetAddress', 'No address')
        estate_id = estate.get('estateId', 'no-id')
        slug = estate_id.lower().replace(" ", "-") if estate_id else "no-slug"

        # Location Information
        object_area = object_address.get('area', 'N/A')
        object_city = object_address.get('city', 'N/A')
        coordinates = object_address.get('coordinate', {})
        object_latitude = str(coordinates.get('latitud', 'N/A'))
        object_longitude = str(coordinates.get('longitud', 'N/A'))

        # Price and Space Information
        price_info = estate.get('price', {})
        price_value = price_info.get('startingPrice', 0)
        formatted_price = f"{int(price_value):,}".replace(",", " ") if price_value else "N/A"
        
        living_space = base_info.get('livingSpace', '0')
        other_space = base_info.get('otherSpace', '0')
        monthly_fee = base_info.get('monthlyFee', '0')

        # Safe number conversions
        living_space_formatted = f"{int(float(living_space))} m²" if living_space else 'N/A'
        other_space_formatted = "0" if not other_space or other_space == '0' else f"{int(float(other_space))} m²"
        monthly_fee_formatted = f"{int(float(monthly_fee)):,}".replace(',', ' ') + ' kr' if monthly_fee else 'N/A'

        # Interior Information
        interior = estate.get('interior', {})
        formatted_rooms = str(int(float(interior.get('numberOfRooms', 0)))) if interior.get('numberOfRooms') is not None else "0"

        # Room Descriptions
        room_descriptions = interior.get('rooms', []) if interior else []
        room_texts = {
            'hall': '',
            'kitchen': '',
            'living_room': '',
            'bedroom': '',
            'bathroom': '',
            'storage': ''
        }
        
        for room in room_descriptions or []:
            heading = room.get('heading', '').lower()
            text = room.get('text', '')
            if heading == 'hall':
                room_texts['hall'] = text
            elif heading == 'kök':
                room_texts['kitchen'] = text
            elif heading == 'vardagsrum':
                room_texts['living_room'] = text
            elif heading == 'sovrum':
                room_texts['bedroom'] = text
            elif heading == 'badrum':
                room_texts['bathroom'] = text
            elif heading == 'förråd':
                room_texts['storage'] = text

        # Building Information
        building = estate.get('building', {})
        building_type = building.get('buildingType', 'N/A')
        building_year = building.get('buildingYear', 'N/A')
        heating_type = building.get('heating', 'N/A')

        # Floor Information
        floor_info = estate.get('floorAndElevator', {})
        elevator = floor_info.get('elevator', 'N/A')
        elevator_formatted = "Ja" if elevator == "Yes" else "Nej" if elevator == "No" else elevator

        # Safe conversions for floor-related values
        try:
            floor = floor_info.get('floor')
            floor_formatted = str(int(float(floor))) if floor is not None else "N/A"
        except (ValueError, TypeError):
            floor_formatted = 'N/A'

        try:
            total_floors = floor_info.get('totalNumberFloors')
            total_floors_formatted = str(int(float(total_floors))) if total_floors is not None else "N/A"
        except (ValueError, TypeError):
            total_floors_formatted = 'N/A'

        # Energy Information
        energy_declaration = estate.get('energyDeclaration', {})
        energy_declaration_date = energy_declaration.get('energyDeclarationDate', 'N/A')
        energy_consumption = energy_declaration.get('energyConsumption', 'N/A')
        energy_class = energy_declaration.get('energyClass', 'N/A')
        selling_heading = estate.get('sellingHeading', '')

        # Inside prepare_webflow_data function, add this near the building info section:
        building = estate.get('building', {})
        other_buildings = sanitize_text(building.get('otherBuildings', 'N/A'), preserve_linebreaks=True)


        # Inside prepare_webflow_data function, add this to get the building info:
        building = estate.get('building', {})
        other_building_info = sanitize_text(building.get('otherAboutTheBuildning', 'N/A'), preserve_linebreaks=True)

        # Apartment Information
        apartment_number_registration = base_info.get('apartmentNumberRegistration', 'N/A')
        fee_commentary = base_info.get('commentary', 'N/A')

        # Participation Information
        participation_info = estate.get('participationAndRepairFund', {})
        
        try:
            participation_fee = participation_info.get('participationOffAnnualFee', 'N/A')
            participation_fee_formatted = f"{participation_fee}%" if participation_fee and participation_fee != 'N/A' else 'N/A'
        except (ValueError, TypeError):
            participation_fee_formatted = 'N/A'

        try:
            participation_in_association = participation_info.get('participationInAssociation', 'N/A')
            participation_in_association_formatted = f"{participation_in_association}%" if participation_in_association and participation_in_association != 'N/A' else 'N/A'
        except (ValueError, TypeError):
            participation_in_association_formatted = 'N/A'

        try:
            indirect_net_debt = participation_info.get('indirectNetDebt', 'N/A')
            indirect_net_debt_formatted = f"{int(indirect_net_debt):,}".replace(",", " ") + " kr" if indirect_net_debt and indirect_net_debt != 'N/A' else 'N/A'
        except (ValueError, TypeError):
            indirect_net_debt_formatted = 'N/A'

        indirect_net_debt_comment = participation_info.get('indirectNetDebtComment', 'N/A')

        # Description Fields
        description = estate.get('description', {})
        print("Raw description data:", description)

        longdescription = sanitize_text(description.get('longSellingDescription', 'No description available'), preserve_linebreaks=True)
        # Modified to use 'generally' field instead of 'other' since that appears to contain the data we want
        descriptionother = sanitize_text(description.get('generally', description.get('other', 'No other description available')), preserve_linebreaks=True)
        short_description = sanitize_text(description.get('shortSellingDescription', 'No description'), preserve_linebreaks=True)
        print("Processed descriptionother:", descriptionother)


        # Surrounding Information - Move this before we try to use it
        surrounding = estate.get('surrounding', {})
        description_area = sanitize_text(
            surrounding.get('generalAboutArea', 'No description available') if surrounding else 'No description available',
            preserve_linebreaks=True
        )

        # Features
        balcony_info = estate.get('balconyPatio', {})
        balcony_present = balcony_info.get('balcony', False)

        # Get main image URL and viewing times
        images = estate.get('images', [])
        main_image_url = get_main_image_url(images)
        viewing_times = format_viewing_times(estate.get('viewings', []))

        webflow_data = {
            "isArchived": False,
            "isDraft": False,
            "fieldData": {
                # Main fields
                "name": street_address,
                "slug": slug,
                "estateid": estate_id,
                "price": formatted_price,
                "address": street_address,
                "monthlyfee": monthly_fee_formatted,
                "livingspace": living_space_formatted,
                "otherspace": other_space_formatted,
                "rooms": formatted_rooms,
                "mainimage": main_image_url,
                "floor": floor_formatted,
                "numberoffloors": total_floors_formatted,
                "visning1": viewing_times[0],
                "visning2": viewing_times[1],
                "visning3": viewing_times[2],
                "sellingheading": estate.get('description', {}).get('sellingHeading', ''),
                "otherbuildings": other_buildings,

                # Room descriptions
                "descriptionhall": sanitize_text(room_texts['hall'], preserve_linebreaks=True),
                "descriptionkitchen": sanitize_text(room_texts['kitchen'], preserve_linebreaks=True),
                "descriptionmainroom": sanitize_text(room_texts['living_room'], preserve_linebreaks=True),
                "descriptionbedroom": sanitize_text(room_texts['bedroom'], preserve_linebreaks=True),
                "descriptionbathroom": sanitize_text(room_texts['bathroom'], preserve_linebreaks=True),
                "descriptionstorage": sanitize_text(room_texts['storage'], preserve_linebreaks=True),

                # Building information
                "buildingtype": building_type,
                "yearbuilt": str(building_year),
                "heatingtype": heating_type,
                "apartmentnumberregistration": apartment_number_registration,
                "elevator": elevator_formatted,

                # Location information
                "objectarea": object_area,
                "objectcity": object_city,
                "latitude": object_latitude,
                "longitude": object_longitude,

                # Association information
                "associationname": assoc_details['name'],
                "associationcorporatenumber": assoc_details['corporate_number'],
                "parking": sanitize_text(assoc_details['parking'], preserve_linebreaks=False),
                "tvbroadband": sanitize_text(assoc_details['tv_and_broadband'], preserve_linebreaks=False),
                "associtationrenovations": assoc_details['renovations'],
                "aboutassociation": assoc_details['about_association'],
                "upplatelseavgift": f"{assoc_details['transfer_fee']}", # Already formatted in get_details_from_association_data
                "commentmonthlyfee": sanitize_text(assoc_details['comment_monthlyfee'], preserve_linebreaks=False),
                "amountapartments": assoc_details['number_of_apartments'],
                "numberlocals": assoc_details['number_of_premises'],
                "otherassociations": assoc_details['other'],
                "ovrigtbyggnad": other_building_info,

                # Additional information
                "feecommentary": fee_commentary,
                "participationoffannualfee": participation_fee_formatted,
                "participationinassociation": participation_in_association_formatted,
                "indirectnetdebt": indirect_net_debt_formatted,
                "indirectnetdebtcomment": indirect_net_debt_comment,

                # Descriptions
                "longdescription": longdescription,
                "areadescription": description_area,
                "descriptionother": descriptionother,
                "bostadsbeskrivning": sanitize_text(short_description, preserve_linebreaks=False),

                # Features
                "balconyorpatio": "Ja" if balcony_present else "Nej"
            }
        }

        # Add highest bid information if available
        highest_bid = find_highest_bid(estate.get('bids', []))
        if highest_bid:
            webflow_data['fieldData'].update({
                "hogstabud": f"{int(highest_bid['amount']):,}".replace(",", " ") + " kr",
                "hogstabuddatum": highest_bid['dateAndTime']
            })

        print("Successfully prepared Webflow data")
        return webflow_data

    except Exception as e:
        print(f"Error preparing Webflow data: {e}")
        import traceback
        traceback.print_exc()
        return None

def find_webflow_item_by_slug(slug: str, collection_id: str) -> Optional[str]:
    """Find a Webflow item by its slug with improved retry logic."""
    print(f"Looking for item with slug: {slug}")
    session = create_retry_session()
    
    try:
        url = f"https://api.webflow.com/v2/collections/{collection_id}/items"
        params = {
            'limit': 100,
            'offset': 0
        }
        
        response_data = make_webflow_request(session, 'GET', url, params=params)
        if not response_data:
            print("Failed to get response from Webflow")
            return None

        items = response_data.get('items', [])
        if not items:
            print("No items found")
            return None

        # Look for matching slug
        for item in items:
            fields = item.get('fieldData', {})
            webflow_slug = fields.get('slug', '')
            
            if webflow_slug and webflow_slug.strip().lower() == slug.strip().lower():
                print(f"Found matching item with ID: {item['id']}")
                return item['id']

        print("No matching item found")
        return None

    except Exception as e:
        print(f"Error searching for item: {str(e)}")
        return None
    finally:
        session.close()

def update_webflow_item(estate: Dict[str, Any], association_data: Dict[str, Any], collection_id: str) -> bool:
    """Update an existing Webflow CMS item."""
    try:
        print("Preparing webflow data for update...")
        webflow_data = prepare_webflow_data(estate, association_data)
        if not webflow_data:
            print("Failed to prepare Webflow data")
            return False

        estate_id = estate.get('estateId', '')
        slug = estate_id.lower().replace(" ", "-") if estate_id else "no-slug"
        
        print(f"Looking for existing item with slug {slug}")
        existing_item_id = find_webflow_item_by_slug(slug, collection_id)
        if not existing_item_id:
            print("No existing item found, will try to create new")
            return False

        print(f"Found existing item, attempting update...")
        session = create_retry_session()
        try:
            url = f"https://api.webflow.com/v2/collections/{collection_id}/items/{existing_item_id}"
            if make_webflow_request(session, 'PATCH', url, json=webflow_data):
                print(f"Successfully updated item")
                return True
            return False
        finally:
            session.close()

    except Exception as e:
        print(f"Error in update process: {str(e)}")
        return False

def create_webflow_item(estate: Dict[str, Any], association_data: Dict[str, Any], collection_id: str) -> bool:
    """Create a new Webflow CMS item."""
    try:
        print("Preparing webflow data for creation...")
        webflow_data = prepare_webflow_data(estate, association_data)
        if not webflow_data:
            print("Failed to prepare Webflow data")
            return False

        estate_id = estate.get('estateId', '')
        slug = estate_id.lower().replace(" ", "-") if estate_id else "no-slug"
        
        print(f"Attempting to create new item with slug {slug}")
        session = create_retry_session()
        try:
            url = f"https://api.webflow.com/v2/collections/{collection_id}/items?live=true"
            if make_webflow_request(session, 'POST', url, json=webflow_data):
                print(f"Successfully created new item")
                return True
            return False
        finally:
            session.close()

    except Exception as e:
        print(f"Error in creation process: {str(e)}")
        return False

def fetch_housing_cooperative(customer_id: str, estate_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Fetch housing cooperative data from Vitec API."""
    try:
        url = f"https://connect.maklare.vitec.net/Estate/GetHousingCooperative?customerId={customer_id}&estateId={estate_id}"
        response = requests.get(url, headers=VITEC_HEADERS)
        response.raise_for_status()
        
        data = response.json()
        if not data:
            print("No data received from housing cooperative endpoint")
            return None, None
            
        association = data.get('association', {})
        association_id = association.get('id') if association else None
        
        if association_id:
            print(f"Found Association ID: {association_id}")
        else:
            print("No association ID found")
            
        return data, association_id

    except requests.exceptions.RequestException as e:
        print(f"Error fetching housing cooperative data: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected error fetching housing cooperative data: {e}")
        return None, None

def fetch_association_details(customer_id: str, association_id: str) -> Optional[Dict[str, Any]]:
    """Fetch association details from Vitec API."""
    try:
        if not association_id:
            print("No association ID provided")
            return None

        url = f"https://connect.maklare.vitec.net/Advertising/Association/{customer_id}/{association_id}"
        response = requests.get(url, headers=VITEC_HEADERS)
        response.raise_for_status()
        
        data = response.json()
        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching association details: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching association details: {e}")
        return None

def sync_estate_data(estate_id: str, customer_id: str, collection_id: str) -> bool:
    """Synchronize a single estate's data with Webflow if status is 'till salu'."""
    try:
        # Fetch estate and association data
        estate_data, association_id = fetch_housing_cooperative(customer_id, estate_id)
        if not estate_data:
            print(f"No estate data found for ID {estate_id}")
            return False

        # Get assignment status with safer dictionary access
        assignment = estate_data.get('assignment', {})
        status = assignment.get('status', {})
        status_id = status.get('id') if isinstance(status, dict) else status
        status_name = status.get('name') if isinstance(status, dict) else "Unknown"

        if status_id == "1":
            print(f"Skipping estate {estate_id} - already sold")
            return False
        elif status_id == "2":
            print(f"Skipping estate {estate_id} - not yet for sale (Intaget)")
            return False
        elif status_id != "3":
            print(f"Skipping estate {estate_id} - unknown status: {status_id} ({status_name})")
            return False

        print(f"Processing estate {estate_id} - status: {status_name}")

        # Fetch association details if available
        association_data = None
        if association_id:
            association_data = fetch_association_details(customer_id, association_id)

        # Try to update existing item first, create new if not found
        # Use estate_data instead of estate
        if not update_webflow_item(estate_data, association_data or {}, collection_id):
            return create_webflow_item(estate_data, association_data or {}, collection_id)
        
        return True

    except Exception as e:
        print(f"Error syncing estate data: {e}")
        return False

    except Exception as e:
        print(f"Error syncing estate data: {e}")
        return False

        # Get ventilation info
        ventilation_data = estate.get('ventilation', {})
        ventilation_type = ventilation_data.get('type', '')
        ventilation_inspection = ventilation_data.get('inspection', '')
        ventilation_info = f"Typ: {ventilation_type}, Inspektion: {ventilation_inspection}" if ventilation_type or ventilation_inspection else ''


        # Status mapping (based on observed values)
        # 1 = "Såld" (Sold)
        # 2 = "Intaget" (Taken/Not yet for sale)
        # 3 = "Till Salu" (For sale)
        if status_id == "1":
            print(f"Skipping estate {estate_id} - already sold")
            return False
        elif status_id == "2":
            print(f"Skipping estate {estate_id} - not yet for sale (Intaget)")
            return False
        elif status_id != "3":
            print(f"Skipping estate {estate_id} - unknown status: {status_id} ({status_name})")
            return False

        print(f"Processing estate {estate_id} - status: {status_name}")

        # Fetch association details if available
        association_data = None
        if association_id:
            association_data = fetch_association_details(customer_id, association_id)

        # Try to update existing item first, create new if not found
        if not update_webflow_item(estate_data, association_data or {}, collection_id):
            return create_webflow_item(estate_data, association_data or {}, collection_id)
        
        return True

    except Exception as e:
        print(f"Error syncing estate data: {e}")
        return False

def process_estates(vitec_data: Dict[str, Any]) -> None:
    """Process all estates from Vitec data."""
    try:
        if not vitec_data or not isinstance(vitec_data, list) or not vitec_data[0]:
            print("Invalid or empty Vitec data")
            return

        housing_cooperatives = vitec_data[0].get('housingCooperativeses', [])
        if not housing_cooperatives:
            print("No housing cooperatives found in data")
            return

        total_count = len(housing_cooperatives)
        print(f"Processing {total_count} housing cooperatives...")
        
        success_count = 0
        processed_count = 0
        
        for cooperative in housing_cooperatives:
            processed_count += 1
            estate_id = cooperative.get('id')
            
            if not estate_id:
                print(f"Skipping cooperative {processed_count}/{total_count} - no estate ID")
                continue

            print(f"\nProcessing cooperative {processed_count}/{total_count} - Estate ID: {estate_id}")
            
            if sync_estate_data(estate_id, VITEC_DATA_PAYLOAD["customerId"], COOPERATIVES_COLLECTION_ID):
                success_count += 1
                print(f"Successfully processed estate {estate_id}")
            else:
                print(f"Failed to process estate {estate_id}")

            print(f"Progress: {processed_count}/{total_count} cooperatives processed, {success_count} successful")

        print(f"\nFinished processing {total_count} cooperatives")
        print(f"Successfully processed: {success_count}")
        print(f"Failed: {total_count - success_count}")

    except Exception as e:
        print(f"Error processing estates: {str(e)}")
        import traceback
        traceback.print_exc()

def main() -> None:
    """Main execution function."""
    try:
        print("Starting Vitec to Webflow synchronization...")
        
        # Validate Webflow collection schema
        schema = fetch_webflow_collection_schema(COOPERATIVES_COLLECTION_ID)
        if not schema:
            print("Failed to validate Webflow collection schema")
            return

        # Fetch estate list from Vitec
        vitec_data = fetch_estate_list()
        if not vitec_data:
            print("Failed to fetch estate list from Vitec")
            return

        # Process estates
        process_estates(vitec_data)

        print("Synchronization completed")

    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        print("Process finished")

# Move all existing code here first
# Keep all function definitions including main()

# Then place this block at the very end of the file
if __name__ == "__main__":
    print("Select Mode:")
    print("1. Full Sync (Automatic)") 
    print("2. Manual Post (Single Estate)")
    mode = input("Enter your choice (1 or 2): ").strip()
    
    if mode == "1":
        main()
    elif mode == "2": 
        main_manual_trigger()
    else:
        print("Invalid choice. Exiting.")


