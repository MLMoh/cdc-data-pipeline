db = db.getSiblingDB('data_pipeline');

db.createCollection('users');

db.users.createIndex({ "_id": 1 }, { unique: true });
db.users.createIndex({ "state": 1 });
db.users.createIndex({ "occupation": 1 });

print('MongoDB initialized successfully');
print('  - Database: data_pipeline');
print('  - Collection: users');
print('  - Indexes: _id (unique), state, occupation');