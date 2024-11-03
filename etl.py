import os
import json
import requests
import pandas as pd
from pprint import pprint
from dotenv import load_dotenv
from pymongo import MongoClient, errors

load_dotenv()

base_url = "https://v3.football.api-sports.io/teams/statistics?league=39&team="
payload={}
headers = {
    'x-rapidapi-key': os.getenv("RAPID_API_KEY"),
    'x-rapidapi-host': os.getenv("RAPID_API_HOST")
}


def extract_from_api():

    extract_teams_data = []
    '''
    for teams in range(33, 43, 1): #starts with teamid 33 until teamid 43 with increment of 1
        url = f"{base_url}{teams}&season=2022"
        req = requests.get(url, headers=headers, data=payload)
        print(req.status_code)
        json_data = req.json()

        extract_teams_data.append(json_data)
        pprint(json_data)
        print()

    print(len(extract_teams_data))
    return extract_teams_data
    '''

    team_list = [33, 34, 35, 40, 42, 46, 47, 49, 50, 51]
    for teams in team_list: 
        url = f"{base_url}{teams}&season=2022"
        req = requests.get(url, headers=headers, data=payload)
        print(req.status_code)
        json_data = req.json()

        extract_teams_data.append(json_data)
        pprint(json_data)
        print()

    print(len(extract_teams_data))
    return extract_teams_data

def transform(extract_teams_data):
    
    # Organize data into a DataFrame
    transform_teams_data = []

    for teams_data in extract_teams_data:
        data = {
                'Team Name': teams_data['response']['team']['name'],
                'League': teams_data['response']['league']['name'],
                'Season': teams_data['response']['league']['season'],
                'Form': teams_data['response']['form'],
                'Total Matches Played': teams_data['response']['fixtures']['played']['total'],
                'Total Wins': teams_data['response']['fixtures']['wins']['total'],
                'Total Draws': teams_data['response']['fixtures']['draws']['total'],
                'Total Losses': teams_data['response']['fixtures']['loses']['total'],
                'Goals Scored': teams_data['response']['goals']['for']['total']['total'],
                'Goals Conceded': teams_data['response']['goals']['against']['total']['total'],
                'Clean Sheets': teams_data['response']['clean_sheet']['total'],
                'Failed to Score': teams_data['response']['failed_to_score']['total'],
                'Penalties Scored': teams_data['response']['penalty']['scored']['total'],
                'Penalties Missed': teams_data['response']['penalty']['missed']['total'],
                'Yellow Cards': sum([val['total'] for val in teams_data['response']['cards']['yellow'].values() if val['total']]),
                'Red Cards': sum([val['total'] for val in teams_data['response']['cards']['red'].values() if val['total']])
            }
        transform_teams_data.append(data)

    transform_teams_data = pd.DataFrame(transform_teams_data)
    print(transform_teams_data)
    return transform_teams_data

def load(transform_teams_data):
    if os.path.exists('epl.json'):
        # Load existing data
        with open('epl.json', 'w') as file: #w means it will overwrite if the file already existed
            #existing_data = pd.read_json(file)
            #combined_data = pd.concat([existing_data, combined_data], ignore_index=True)
            transform_teams_data.to_json("epl.json", orient="records", indent=4)

    else:
        print("File doesn't yet exist. Creating 'epl.json'.")
        #combined_data = transform_teams_data
        transform_teams_data.to_json("epl.json", orient="records", indent=4)

    # Save the combined data back to 'epl.json'
    #combined_data.to_json("epl.json", orient="records", indent=4)



    #myclient = MongoClient("mongodb://localhost:27017/")
    #mydb = myclient["football"] #database
    #epl = mydb["epl"] #collection

    # Convert DataFrame to a list of dictionaries and insert into MongoDB
    epl_data = transform_teams_data.to_dict('records')
    #epl.insert_many(epl_data)



    try:
        myclient = MongoClient(os.getenv("MONGODB_URI"))
        mydb = myclient["football"]
        epl = mydb["epl"]
        print(mydb.list_collection_names())
        #collection_list = mydb.list_collections()
        #for c in collection_list:
            #print(c)

        
        if "epl" in mydb.list_collection_names():
            print("The collection exist")
            epl.drop()
            print("Successfully dropped collections")
            epl_data = transform_teams_data.to_dict('records')
            epl.insert_many(epl_data)

        else:
            epl_data = transform_teams_data.to_dict('records')
            epl.insert_many(epl_data)
            print("Collection doesnt exist yet and creating a new one")
        

    except errors.ConnectionFailure as e:
        print("An error occurred with MongoDB connection:", e)

    


etd = extract_from_api()
ttd = transform(etd)
load(ttd)