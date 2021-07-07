import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import tqdm
import random
import time


from faker import Faker
fake = Faker()

# npi X year X practice group X provider type X patient panel
# county X cbsa X zip X state

PROVIDERS_INDEX_NAME = 'providers'
LOCATION_SUGGESTIONS_INDEX_NAME = 'locations-suggestions'
NON_LOCATION_SUGGESTIONS_INDEX_NAME = 'general-suggestions'

NUMBER_OF_DOCS = 10_000

EPISODE_TYPES = ['Diskectomy', 'Osteoarthritis', 'Hip/Pelvic Fracture']
SPECIALTIES = [
    "Internal Medicine",
    "Physician Assistant",
    "Family Practice",
    "General Practice",
    "Diagnostic Radiology",
    "Clinical Laboratory",
    "Thoracic Surgery",
    "Emergency Medicine",
    "Plastic And Reconstructive Surgery",
    "Speech Language Pathologist",
    "Medical Oncology",
    "Radiation Therapy Centers",
    "All Other Suppliers (Drug/Dept Stores)",
    "Mammography Screening Center",
    "Individual Certified Prosthetist",
    "Medical Supply Company With Certified Prosthetist-Orthotist",
    "Hematology/Oncology",
    "Optometrist",
    "Obstetrics/Gynecology",
    "General Surgery",
    "Certified Nurse Midwife",
    "Certified Clinical Nurse Specialist",
    "Cardiac Surgery",
    "Public Health Or Welfare Agencies",
    "Registered Dietician/Nutrition Professional",
    "Otolaryngology",
    "Critical Care (Intensivists)",
    "Dentist",
    "Psychologist",
    "Pediatric Medicine",
    "Vascular Surgery",
    "Audiologist",
    "Gastroenterology",
    "Mass Immunization Roster Biller",
    "Pain Management",
    "Rheumatology",
    "Sleep Medicine",
    "Interventional Radiology",
    "Hematopoietic Cell Transplantation And Cellular Therapy",
    "Geriatric Psychiatry Colorectal Surgery",
    "Hospital (Dmercs Only)",
    "Medical Supply Company With Certified Prosthetist",
    "Nephrology",
    "Dermatology",
    "Clinical Psychologist",
    "Infectious Disease",
    "Ambulance Service Supplier",
    "Cardiac Electrophysiology",
    "Unknown",
    "Peripheral Vascular Disease",
    "Neurosurgery",
    "CRNA",
    "Radiation Oncology",
    "Chiropractic",
    "Endocrinology",
    "Anesthesiologist Assistants",
    "Oral Surgery (Dentists Only)",
    "Sports Medicine",
    "Preventive Medicine",
    "Slide Preparation Facilities",
    "Addiction Medicine",
    "Intensive Cardiac Rehabilitation",
    "Pathology",
    "Urology",
    "Licensed Clinical Social Worker",
    "Geriatric Medicine",
    "Anesthesiology",
    "Neurology",
    "Podiatry",
    "Orthopedic Surgery",
    "Occupational Therapist",
    "Pulmonary Disease",
    "Maxillofacial Surgery",
    "Colorectal Surgery",
    "Surgical Oncology",
    "Interventional Pain Management",
    "Osteopathic Manipulative Therapy",
    "Interventional Cardiology",
    "Gynecologist/Oncologist",
    "Independent Diagnostic Testing Facility (Idtf)",
    "Medical Supply Company For Dmerc",
    "Hospitalist",
    "Physical Therapist",
    "Pharmacy (Dmerc)",
    "Centralized Flu",
    "Hospice And Palliative Care",
    "Nuclear Medicine",
    "Voluntary Health Or Charitable Agencies",
    "Neuropsychiatry",
    "Nurse Practitioner",
    "Cardiology",
    "Orthopaedic",
    "Ambulatory Surgical Center",
    "Hand Surgery",
    "Medical Toxicology",
    "Multispecialty Clinic Or Group Practice",
    "Physical Medicine And Rehabilitation",
    "Psychiatry",
    "Portable X-Ray Supplier",
    "Advanced Heart Failure And Transplant Cardiology",
    "Medical Supply Company With Registered Pharmacist",
    "Ophthalmology",
    "Hematology",
    "Allergy/Immunology",
]
CJ_SPECIALTIES = {    
    "Orthopaedic": [
        "Orthopaedic - Surgery Orthopaedic Surgery of the Spine",
        "Orthopaedic - Surgery Shoulder (Custom)",
        "Orthopaedic - Surgery Hand Surgery",
        "Orthopaedic - Surgery Foot and Ankle Surgery",
        "Orthopaedic - Surgery Adult Reconstructive Orthopaedic Surgery"
    ],
    "Cardiology": [
        "Cardiology - General",
        "Cardiology - ACHD",
        "Cardiology - Electrophysiology",
        "Cardiology - Transplant",
        "Cardiology - Interventional",
        "Cardiology - Imaging",
        "Cardiology - Vascular"
    ]
}
# from here: https://public.opendatasoft.com/explore/dataset/core-based-statistical-areas-cbsas-and-combined-statistical-areas-csas/table/"
j = open('core-based-statistical-areas-cbsas-and-combined-statistical-areas-csas.json', 'r').read()
j = json.loads(j)
# from here: https://public.opendatasoft.com/explore/dataset/geonames-postal-code/table/?q=&refine.country_code=US and edited the fields
zips_states = open('zip_city_county_state.json', 'r').read()
zips_states = json.loads(zips_states)
cities_by_state = open('cities_by_state.json', 'r').read()
cities_by_state = json.loads(cities_by_state)


# CBSAs = list(set([r['fields'].get('cbsa_title') for r in j]))
# CBSA file seems to have a few cbsa title missing, which is introducing a None value in the CBSA array. Im removing it here



# COUNTIES = list(set([r['fields'].get('county_county_equivalent') for r in j]))
STATES = list(set([r['fields'].get('state_name') for r in j]))

STATE_NAMES = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]
COUNTY_NAMES = ['Fairfax', 'Arlington', 'Loudon', 'Richmond']

EDGE_CASE_FIELDS = ['FULL_NAME', 'PATIENT_VOLUME', 'PPI_COST_SCORE', 'PPI_OUTCOME_SCORE', 'PRACTICE_GROUP']


def get_fake_full_name():
    return fake.name()

def get_fake_first_name():
    return fake.name().split()[0]

def get_fake_last_name():
    return fake.name().split()[1]

FAKE_NAMES = [fake.name() for i in range(1000)]
USED_FAKE_NAMES = []
FAKE_PRACTICE_GROUPS = [fake.company() for i in range(1000)]
USED_FAKE_PRACTICE_GROUPS = []

def random_select_name():
    n = random.choice(FAKE_NAMES)
    USED_FAKE_NAMES.append(n)
    return n

def random_select_practice_group():
    pg = random.choice(FAKE_PRACTICE_GROUPS)
    USED_FAKE_PRACTICE_GROUPS.append(pg)
    return pg

def data_by_states():
    DATA_BY_STATES = dict()
    COUNTIES_BY_STATES = []
    CBSA_BY_STATES = []
    for state_name in STATES:
        state_data = list(filter(lambda x: x['fields'].get('state_name') == state_name, j))
        CBSA_BY_STATES = [i['fields'].get('cbsa_title') for i in state_data]
        DATA_BY_STATES[state_name] = {
            'cbsas': CBSA_BY_STATES,
            'state_data': state_data
        }
    return DATA_BY_STATES

STATES_DATA = data_by_states()

def generate_address_data(doc):
    states = random.choices(STATES, k=random.randint(1,6))
    counties = []
    CBSAS = []
    cities = []
    zip_codes = []
    full_address_list = []
    for state in states:
        address = fake.street_address()
        CBSA = random.choice(STATES_DATA[state]['cbsas'])
        counties_by_cbsa_and_state = list(filter(lambda x: x['fields'].get('state_name') == state and x['fields'].get('cbsa_title') == CBSA, STATES_DATA[state]['state_data']))
        county = random.choice(counties_by_cbsa_and_state)
        county = county['fields']['county_county_equivalent']
        cities_zips_by_county = list(filter(lambda x: x['state'] == state and x['county'] == county, zips_states))
        if not cities_zips_by_county:
            continue
        city_zip_by_county = random.choice(cities_zips_by_county)
        full_address = full_address = "%s, %s, %s, %s | %s" % (address, city_zip_by_county['city'], state, city_zip_by_county['zip_code'], CBSA)

        CBSAS.append(CBSA)
        counties.append(county)
        cities.append(city_zip_by_county['city'])
        zip_codes.append(city_zip_by_county['zip_code'])
        full_address_list.append(full_address)
    doc['CBSA'] = CBSAS
    doc['ADDRESS_COUNTY'] = counties
    doc['ADDRESS_CITY'] = cities
    doc['ADDRESS_ZIP_CODE'] = zip_codes
    doc['ADDRESS_FULL'] = full_address_list
    doc['ADDRESS_STATE'] = states
    return doc

def generate_specialty(doc):
    specialty = random.choice(SPECIALTIES)
    doc['SPECIALTY'] = specialty
    if specialty in CJ_SPECIALTIES:
        doc['CJ_SPECIALTY'] = random.choice(CJ_SPECIALTIES[specialty])
    return doc

fake_doc_generator = dict()
#fields that use faker
fake_doc_generator['NPI'] = lambda: str(random.randint(1000_000_000, 9_000_000_000))
fake_doc_generator['PROVIDER_TYPE'] = lambda: random.choice(['Specialist', 'PCP'])
fake_doc_generator['PATIENT_PANEL'] = lambda: random.choice(['Medicare Fee for Service', 'Commercial'])
fake_doc_generator['YEAR_OF_SERVICE'] = lambda: random.choice([2019, 2020, 2021])
fake_doc_generator['FULL_NAME'] = random_select_name
fake_doc_generator['FIRST_NAME'] = get_fake_first_name
fake_doc_generator['LAST_NAME'] = get_fake_last_name
fake_doc_generator['PPI_COST_SCORE'] = lambda: random.randint(0,5)
fake_doc_generator['PPI_OUTCOME_SCORE'] = lambda: random.randint(0,5)
fake_doc_generator['EPISODES'] = lambda: random.choices(EPISODE_TYPES, k=4)
fake_doc_generator['PRACTICE_GROUP'] = random_select_practice_group
fake_doc_generator['NPI_COST_PER_PATIENT_PER_YEAR'] = lambda: random.randint(0,20)
fake_doc_generator['CBSA_COST_PER_PATIENT_PER_YEAR'] = lambda: random.randint(0,20)
fake_doc_generator['AVERAGE_COST_PER_EPISODE'] = lambda: random.randint(0,2000)
fake_doc_generator['RISK_SCORE'] = lambda: random.random()
fake_doc_generator['NUMBER_OF_EPISODES'] = lambda: random.randint(0,200)
fake_doc_generator['PATIENT_VOLUME'] = lambda: random.randint(0,2000)

def generate_provider_docs():
    fields = fake_doc_generator.keys()
    for _ in range(NUMBER_OF_DOCS):
        doc = {}
        for f in fields:
            doc[f] = fake_doc_generator[f]()
        doc = generate_address_data(doc)
        doc = generate_specialty(doc)
        yield doc

def generate_edge_cases():
    fields = fake_doc_generator.keys()
    docs = list()
    for field in EDGE_CASE_FIELDS:
        doc_missing_fields = {}
        doc_empty_strings = {}
        for f in fields:
            doc_missing_fields[f] = fake_doc_generator[f]()
            doc_missing_fields['SPECIALTY'] = 'EDGECASE ' + field
            doc_missing_fields['PROVIDER_TYPE'] = 'Specialist'
            doc_missing_fields['PATIENT_PANEL'] = 'Commercial'
            doc_missing_fields['YEAR_OF_SERVICE'] = 2020

            # Adding a copy of the doc with empty string instead of missing field
            doc_empty_strings = doc_missing_fields.copy()
            doc_empty_strings[field] = ''
            doc_missing_fields.pop(field, None)
        docs.append(doc_empty_strings)
        docs.append(doc_missing_fields)
    return iter(docs)
    

def generate_location_suggestions():
    for i in j:
        yield dict(SUGGESTION_TYPE='CBSA', SUGGESTION_VALUE=i['fields'].get('cbsa_title'), LOCATION_STATE=i['fields'].get('state_name'))
    for i in j:
        yield dict(SUGGESTION_TYPE='ADDRESS_COUNTY', SUGGESTION_VALUE=i['fields'].get('county_county_equivalent'), LOCATION_STATE= i['fields'].get('state_name'))
    for i in zips_states:
        yield dict(SUGGESTION_TYPE='ADDRESS_ZIP_CODE', SUGGESTION_VALUE=str(i['zip_code']), LOCATION_STATE=i['state'])
    for state, cities in cities_by_state.items():
        for city in cities:
            yield dict(SUGGESTION_TYPE='ADDRESS_CITY', SUGGESTION_VALUE=city, LOCATION_STATE=state)
    for i in STATE_NAMES:
        yield dict(SUGGESTION_TYPE='ADDRESS_STATE', SUGGESTION_VALUE=i, LOCATION_STATE='#NA')

def generate_general_suggestions():
    for i in EPISODE_TYPES:
        yield dict(SUGGESTION_TYPE='EPISODES', SUGGESTION_VALUE=i)
    for i in SPECIALTIES:
        yield dict(SUGGESTION_TYPE='SPECIALTY', SUGGESTION_VALUE=i)
    for SPEC, CJ_SPECS in CJ_SPECIALTIES.items():
        for i in CJ_SPECS:
            yield dict(SUGGESTION_TYPE='CJ_SPECIALTY', SUGGESTION_VALUE=i)
    for i in range(len(USED_FAKE_NAMES)):
        yield dict(SUGGESTION_TYPE='FULL_NAME', SUGGESTION_VALUE=USED_FAKE_NAMES[i])
    for i in range(len(USED_FAKE_PRACTICE_GROUPS)):
        yield dict(SUGGESTION_TYPE='PRACTICE_GROUP', SUGGESTION_VALUE=USED_FAKE_PRACTICE_GROUPS[i])



def main():
    time.sleep(60)
    es = Elasticsearch([{'host': 'elastic-search', 'port': 9200}])
    print('Creating the location suggestions index')
    mappings = {
        "mappings": {
            "properties" : {
                "SUGGESTION_TYPE": {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'keyword', 
                            'ignore_above': 256
                        }
                    }
                },
                "SUGGESTION_VALUE": {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'keyword', 
                            'ignore_above': 256
                        },
                        'search_as_you_type_field': {
                            "type": "search_as_you_type",
                        }
                    }
                },
                "LOCATION_STATE": {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'keyword', 
                            'ignore_above': 256
                        }
                    }
                },
            }
        }
    }
    es.indices.create(LOCATION_SUGGESTIONS_INDEX_NAME, body=mappings, ignore=400)  # ignore 'already exists' exception

    print('Creating the general suggestions index')
    mappings = {
        "mappings": {
            "properties" : {
                "SUGGESTION_TYPE": {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'keyword', 
                            'ignore_above': 256
                        }
                    }
                },
                "SUGGESTION_VALUE": {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'keyword', 
                            'ignore_above': 256
                        },
                        'search_as_you_type_field': {
                            "type": "search_as_you_type",
                        }
                    }
                }
            }
        }
    }
    es.indices.create(NON_LOCATION_SUGGESTIONS_INDEX_NAME, body=mappings, ignore=400)  # ignore 'already exists' exception

    print('Creating "providers" index')
    mappings = {
        "mappings": {
            "properties": {
                "SPECIALTY": {
                    "type": "text",
                    "index_prefixes": {},
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "CJ_SPECIALTY": {
                    "type": "text",
                    "index_prefixes": {},
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "EPISODES": {
                    "type": "text",
                    "index_prefixes": {},
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "CBSA": {
                    "type": "text",
                    "index_prefixes": {},
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ADDRESS_COUNTY": {
                    "type": "text",
                    "index_prefixes": {},
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ADDRESS_STREET": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ADDRESS_STATE": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ADDRESS_ZIP_CODE": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "PRACTICE_GROUP": {
                    "type": "text",
                    "index_prefixes": {},
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "FIRST_NAME": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "LAST_NAME": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "FULL_NAME": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "NPI": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "YEAR_OF_SERVICE": {
                    "type": "integer",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "PROVIDER_TYPE": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "PATIENT_PANEL": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "PPI_COST_SCORE": {
                    "type": "integer",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "PPI_OUTCOME_SCORE": {
                    "type": "integer",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "RISK_SCORE": {
                    "type": "double"
                },
                "NUMBER_OF_EPISODES": {
                    "type": "double"
                },
                "NPI_COST_PER_PATIENT_PER_YEAR": {
                    "type": "double"
                },
                "CBSA_COST_PER_PATIENT_PER_YEAR": {
                    "type": "double"
                },
                "PATIENT_VOLUME": {
                    "type": "double"
                },
                "AVERAGE_COST_PER_EPISODE": {
                    "type": "double"
                }
            }
        }
    }
    es.indices.create(PROVIDERS_INDEX_NAME, body=mappings, ignore=400)  # ignore 'already exists' exception

    # example suggestion query
    # POST http://localhost:9200/providers/_search
    # {"_source": "suggest", "suggest": {"my-suggestion": {"prefix": "orth", "completion": {"field": "suggest", "size": 10, "fuzzy": {"fuzziness": 2}}}}}

    # {"query": {"multi_match": {"query": "brown f", "fuzziness": "AUTO", "type": "bool_prefix","fields": ["search_as_you_type_field", "search_as_you_type_field._2gram", "search_as_you_type_field._3gram"]}}}
    # NOTE: fuzziness will not work with just one word
    # "The fuzziness , prefix_length , max_expansions , rewrite , and fuzzy_transpositions parameters are supported for the terms that are used to construct term queries, but do not have an effect on the prefix query constructed from the final term."
    # {"collapse": {"field": "specialty.keyword"}, "query": {"multi_match": {"query": "Orthopedic Sirgery", "fuzziness": "AUTO", "type": "bool_prefix","fields": ["specialty.search_as_you_type_field", "specialty.search_as_you_type_field._2gram", "specialty.search_as_you_type_field._3gram", "specialty.search_as_you_type_field._index_prefix"]}}}

    # dynamic mapping created by es
    # {'providers': {'mappings': {'properties': {'address': {'properties': {'city': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'full': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'state': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 
    # 'street': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'zipCode': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}}}, 
    # 'averageHCC': {'type': 'float'}, 'cbsa': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'costIndex': {'type': 'long'}, 
    # 'name': {'properties': {'display': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'firstname': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'full': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'lastname': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}}}, 
    # 'npi': {'type': 'long'}, 'numEpisodes': {'type': 'long'}, 'outcomeIndex': {'type': 'long'}, 'pmpy': {'type': 'long'}, 
    # 'practiceGroup': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'procedures': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 
    # 'regions': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 
    # 'specialty': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}, 'index_prefixes': {'min_chars': 2, 'max_chars': 5}}, 'suggest': {'type': 'completion', 'analyzer': 'simple', 'preserve_separators': True, 'preserve_position_increments': True, 'max_input_length': 50}, 'year': {'type': 'long'}}}}}

    print('Indexing the main index documents...')
    progress = tqdm.tqdm(unit="docs", total=NUMBER_OF_DOCS)
    successes = 0

    for ok, action in streaming_bulk(
        client=es, index=PROVIDERS_INDEX_NAME, actions=generate_provider_docs(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, NUMBER_OF_DOCS))

    print('Indexing the edge cases index documents...')
    progress = tqdm.tqdm(unit="docs", total=len(EDGE_CASE_FIELDS))
    successes = 0

    for ok, action in streaming_bulk(
        client=es, index=PROVIDERS_INDEX_NAME, actions=generate_edge_cases(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, len(EDGE_CASE_FIELDS)))

    print('Indexing location suggestions...')
    progress = tqdm.tqdm(unit="docs", total=NUMBER_OF_DOCS)
    successes = 0
    for ok, action in streaming_bulk(
        client=es, index=LOCATION_SUGGESTIONS_INDEX_NAME, actions=generate_location_suggestions(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, NUMBER_OF_DOCS))

    print('Indexing general suggestions...')
    progress = tqdm.tqdm(unit="docs", total=NUMBER_OF_DOCS)
    successes = 0
    for ok, action in streaming_bulk(
        client=es, index=NON_LOCATION_SUGGESTIONS_INDEX_NAME, actions=generate_general_suggestions(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, NUMBER_OF_DOCS))

if __name__ == '__main__':
    main()