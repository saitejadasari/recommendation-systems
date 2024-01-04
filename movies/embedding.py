from sentence_transformers import SentenceTransformer

# Load the pre-trained model
def get_model():
  return SentenceTransformer('all-MiniLM-L12-v2')

