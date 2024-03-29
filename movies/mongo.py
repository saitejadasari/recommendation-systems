import pymongo
from embedding import get_model
import streamlit as st


# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(st.secrets["mongo_srv"])

client = init_connection()

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

def count_documents(database, collection, query):
  coll = client[database][collection]
  return coll.count_documents(query)

def find_query(database, collection, query, projection=None, limit=10, skip=0):
  coll = client[database][collection]
  return coll.find(query, projection, limit=limit, skip=skip)



def find_one_query(database, collection, query):
  coll = client[database][collection]
  return coll.find_one(query)


def aggregate_query(database, collection, index, path, query_vector, projection=None, numCandidates=100, limit=10):
  
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
  print("Pipeline query", pipe)
  coll = client[database][collection]
  return coll.aggregate(pipe)
