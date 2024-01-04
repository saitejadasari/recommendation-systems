from embedding import get_model
from mongo import aggregate_query, find_query, create_embedding, create_vector_index


def search_movie(movieName):
    query = {'title': '/.*' + movieName + '.*/i'}
    projection = {'_id': 0, 'title': 1, 'genres': 1, 'plot': 1, 'year': 1}
    cursor = find_query('sample_mflix', 'movies', query, projection)
    return list(cursor)


def recommend_movies(movieName, distance = "cosine"):
    index = 'fullplot_embed_cosine' if distance == "cosine" else 'fullplot_embed'
    model = get_model()
    query_vector = model.encode(movieName)
    cursor = aggregate_query('sample_mflix', 'embedding_movies', index, 'fullplot_embedding', query_vector.tolist())
    return list(cursor)


# create embeddings for all the movies
# this is a one time operation
def create_embeddings():
    print("Creating embeddings")
    create_embedding('sample_mflix', 'movies')
    create_vector_index('sample_mflix', 'embedding_movies', 'fullplot_embedding', 'fullplot_embed_cosine')


def main():
    # main function
    movies = recommend_movies("The Matrix")
    print(movies)
    pass

if __name__ == '__main__':
    # main function
    main()