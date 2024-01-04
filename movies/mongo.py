import pymongo
from embedding import get_model

# connect to your Atlas cluster
client = pymongo.MongoClient("mongodb+srv://mongo:mongo4kent@cluster0.lqtsseb.mongodb.net/")

# create a new embeddings and set the field in the document
def create_embedding(database, collection):
  coll = client[database][collection]
  print("Total count", coll.count_documents({"fullplot": {"$exists": True}}))
  cur = coll.find({"fullplot": {"$exists": True}})
  for i, el in enumerate(cur):
    # print("index", i)
    title = el['title']
    plot = el['fullplot']
    # print(el['_id'], plot)
    inp = title + " " + plot
    model = get_model()
    embedding = model.encode(inp)
    # print("Embedding", embedding.size)
    coll.update_one({"_id": el['_id']}, {"$set": {"fullplot_embedding": embedding.tolist()}})
  print("DONE!!")


def create_vector_index(database, collection, path, index):
  client[database][collection].create_search_index(
    {
      "definition":
        {
          "mappings": {
            "dynamic": True, 
            "fields": {
            path : {
                "dimensions": 384, # size of the embedding model
                "similarity": "cosine", # 'euclidean' or 'dotproduct'
                "type": "vector" # 'vector' or 'knnVector'
                }
              }
            }
          },
          "name": index
    }
)


def find_query(database, collection, query, projection=None):
  coll = client[database][collection]
  return coll.find(query, projection)



def find_one_query(database, collection, query):
  coll = client[database][collection]
  return coll.find_one(query)


def aggregate_query(database, collection, index, path, query_vector, projection, numCandidates=100, limit=10):
  
  if projection is None:
    projection = {
      '_id': 0,
      'title': 1,
      'genres': 1,
      'plot': 1,
      'year': 1,
      'score': {
        '$meta': 'vectorSearchScore'
      }
    }
  else:
    projection['score'] = {'$meta': 'vectorSearchScore'}
  pipe = [
      {
        '$vectorSearch': {
          'index': index,
            'path': path,
            'queryVector': query_vector,
          'numCandidates': numCandidates,
          'limit': limit
        }
      }, {
        '$project': projection
      }
    ]
  coll = client[database][collection]
  return coll.aggregate(pipe)
