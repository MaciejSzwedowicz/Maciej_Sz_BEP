import pytest
from pymongo import MongoClient

# === CONFIG ===
DB_NAME = "openfda_sample"
COLLECTION_NAME = "sample_reports"
MONGO_URI = "mongodb://localhost:27017/"

# === FIXTURE ===
@pytest.fixture(scope="module")
def collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]

# === TESTS ===

def test_total_documents_exist(collection):
    assert collection.count_documents({}) > 0, "No documents found in the collection"

def test_safetyreportid_exists(collection):
    count = collection.count_documents({"safetyreportid": {"$exists": True}})
    assert count > 0, "No documents have safetyreportid"

def test_patient_drug_exists(collection):
    count = collection.count_documents({"patient.drug": {"$exists": True}})
    assert count > 0, "No documents have patient.drug"

def test_patient_reaction_exists(collection):
    count = collection.count_documents({"patient.reaction": {"$exists": True}})
    assert count > 0, "No documents have patient.reaction"

def test_patient_field_present(collection):
    missing = collection.count_documents({"patient": {"$exists": False}})
    assert missing == 0, f"Found {missing} documents missing patient field"

def test_empty_reactions(collection):
    empty = collection.count_documents({"patient.reaction": []})
    assert empty == 0, f"Found {empty} documents with empty reactions"

def test_query_reactions_with_rash(collection):
    rash_count = collection.count_documents({"patient.reaction.reactionmeddrapt": {"$regex": "rash", "$options": "i"}})
    assert rash_count > 0, "No reports contain 'rash' in reactionmeddrapt"
