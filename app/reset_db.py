from app import db
db.drop_all()
db.create_all()
print("✅ Database reset successfully.")
