import pymongo

def get_db( ):
    with open('.secretmongo') as f:
        password = f.read().strip()
    
    client = pymongo.MongoClient(f"mongodb+srv://daihong:{password}@cluster0-lpkyf.mongodb.net/test?retryWrites=true&w=majority")
    db = client.uscoccer

    return db


def insert_mongo(db, inserttable):

    return db.soccerbackup.insert_many(inserttable.to_dict("recrods"))

