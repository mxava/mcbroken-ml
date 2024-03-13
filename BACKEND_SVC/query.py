import sqlalchemy as db

engine = db.create_engine('dialect+driver://user:pass@host:port/db')
connection = engine.connect()
metadata = db.MetaData()

# ML Model Functions
