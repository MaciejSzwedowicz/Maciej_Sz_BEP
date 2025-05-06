from pymongo import MongoClient

# === CONFIG ===
DB_NAME = "openfda_sample"
COLLECTION_NAME = "sample_reports"
MONGO_URI = "mongodb://localhost:27017/"

# === CONNECT ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print("\n🔍 Running MongoDB Sanity Checks\n")

# === 1. Basic Count ===
total_docs = collection.count_documents({})
print(f"📄 Total documents: {total_docs}")

# === 2. Sample Document ===
print("\n📌 Sample document:")
# print(collection.find_one())

# === 3. Field Presence Checks ===
print("\n🔎 Field checks:")
print(" - With safetyreportid:", collection.count_documents({"safetyreportid": {"$exists": True}}))
print(" - With patient.drug:", collection.count_documents({"patient.drug": {"$exists": True}}))
print(" - With patient.reaction:", collection.count_documents({"patient.reaction": {"$exists": True}}))

# === 4. Null / Empty Values ===
print("\n⚠️ Data quality checks:")
print(" - Missing patient:", collection.count_documents({"patient": {"$exists": False}}))
print(" - Empty reactions:", collection.count_documents({"patient.reaction": []}))

# === 5. Query Example ===
print("\n🔬 Sample query (reactions containing 'rash'):")
rash_reports = collection.find({"patient.reaction.reactionmeddrapt": {"$regex": "rash", "$options": "i"}}).limit(3)
for report in rash_reports:
    print(" - Report ID:", report.get("safetyreportid"))

print("\n✅ Done.")
