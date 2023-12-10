import pymongo

from pymongo import MongoClient
import json

DATABASE_NAME = 'company'
COLLECTION_NAME = 'candidate'

import json
#file path to your JSON file
file_path = "/Users/saharafarah/Downloads/mock_data.json"

# Read the JSON file
with open(file_path, 'r') as json_file:
    data = json.load(json_file)

def insert_mock_data():
    client = MongoClient('localhost', 27017)
  
    
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    inserted_ids = collection.insert_many(data).inserted_ids
    for idx, _id in enumerate(inserted_ids):
        print(f"Inserted document {idx + 1} with ObjectId: {_id}")

        
    """Inserts the generated Mock data in JSON file into the MongoDB."""
    




def find_candidate_numbers():
    """Find the total number of candidates and the number of candidates per state with sort by count in ascending order """
    client = MongoClient('localhost', 27017)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$group": {
                "_id": "$state",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": 1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    total_candidates = sum(result["count"] for result in results)
    
    print("Total number of candidates:", total_candidates)
    print("Number of candidates per state (sorted by count in ascending order):")
    
    for result in results:
        print(f"state: {result['_id']}, Count:{result['count']}")
    



def find_skill_frequencies():
    """Find the skills and their frequencies sorted by frequency in descending order. """
    client = MongoClient('localhost', 27017)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$unwind": "$skills"
        },
        {
            "$group": {
                "_id": "$skills",
                "frequency": {"$sum": 1}
            }
        },
        {
            "$sort": {"frequency": -1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    print("Skills and their frequencies (sorted by frequency in descending order):")
    
    for result in results:
        print(f"Skill: {result['_id']}, Frequency: {result['frequency']}")



def ca_relocation_3skills():
    """Count and find the candidates in California who want to relocate and have more than 3 skills. """
    client = MongoClient('localhost', 27017)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    
    pipeline = [
        {
            "$match": {
                "state": "California",
                "willing_to_relocate": True
            }
        },
        {
            "$project": {
                "name": 1,
                "skills": 1,
                "num_skills": {"$size": "$skills"}
            }
        },
        {
            "$match": {
                "num_skills": {"$gt": 3}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    print(f"Count of candidates in California willing to relocate with more than 3 skills: {len(results)}")
    
    if len(results) > 0:
        print("Candidates' information:")
        for result in results:
            print("Name:", result["name"])
            print("Skills:", result["skills"])
            print("---")




def ten_states_sql():
    """Find the top ten states with the most SQL skill based candidates """
    client = MongoClient('localhost', 27017)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$unwind": "$skills"
        },
        {
            "$match": {
                "skills": "SQL"
            }
        },
        {
            "$group": {
                "_id": "$state",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        {
            "$limit": 10
        }
    ]

    results = list(collection.aggregate(pipeline))

    print("Top ten states with the most SQL skill-based candidates:")
    
    for idx, result in enumerate(results, 1):
        print(f"Rank: {idx}, State: {result['_id']}, SQL Skill Candidate Count: {result['count']}")



def senior_skills_texas():
    """Counts and find the candidates who have senior level experience in Java or R and living in Texas. """
    client = MongoClient('localhost', 27017)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$match": {
                "state": "Texas",
                "$or": [
                    {"skills": "Java", "experience_level": "senior"},
                    {"skills": "R", "experience_level": "senior"}
                ]
            }
        },
        {
            "$project": {
                "name": 1,
                "skills": 1,
                "experience_level": 1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    print(f"Count of candidates in Texas with senior-level experience in Java or R: {len(results)}")
    
    if len(results) > 0:
        print("Candidates' information:")
        for result in results:
            print("Name:", result["name"])
            print("Skills:", result["skills"])
            print("Experience Level:", result["experience_level"])
            print("---")




def ml_junior_senior_nyc():
    """Count and find the candidates who have Machine Learning specialty and senior or junior level experience living in New York City."""
    client = MongoClient('localhost', 27017)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$match": {
                "state": "New York",
                "city": "New York City",
                "specialty": "Machine Learning",
                "$or": [
                    {"experience_level": "senior"},
                    {"experience_level": "junior"}
                ]
            }
        },
        {
            "$project": {
                "name": 1,
                "specialty": 1,
                "experience_level": 1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    print(f"Count of candidates in New York City with Machine Learning specialty and senior or junior-level experience: {len(results)}")
    
    if len(results) > 0:
        print("Candidates' information:")
        for result in results:
            print("Name:", result["name"])
            print("Specialty:", result["specialty"])
            print("Experience Level:", result["experience_level"])
            print("---")



if __name__ == '__main__':

    # database name company
    db = MongoClient().company

    #insert_mock_data()
    #find_candidate_numbers()
    #find_skill_frequencies()
    #ca_relocation_3skills()
    #ten_states_sql()
    #senior_skills_texas()
    ml_junior_senior_nyc()

