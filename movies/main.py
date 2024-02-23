from embedding import get_model
from mongo import aggregate_query, find_query, create_embedding, create_vector_index, count_documents
import streamlit as st


def count_movies():
    return count_documents('sample_mflix', 'movies', {})

def search_movie(movieName):
    
    print("Searching for movie", movieName.strip())
    query = {'title': {"$regex": '.*' + movieName.strip() + '.*', "$options": 'i'}}
    projection = {'_id': 0, 'title': 1, 'genres': 1, 'plot': 1, 'year': 1}
    cursor = find_query('sample_mflix', 'movies', query, projection)
    return list(cursor)


def recommend_movies(movieName, distance = "cosine"):
    print("Getting Recommendations for movie", movieName)
    index = 'fullplot_embed_cosine' if distance == "cosine" else 'fullplot_embed'
    model = get_model()
    query_vector = model.encode(movieName.strip())
    cursor = aggregate_query('sample_mflix', 'embedded_movies', index, 'fullplot_embedding', query_vector.tolist())
    return list(cursor)


# create embeddings for all the movies
# this is a one time operation
def create_embeddings():
    print("Creating embeddings")
    create_embedding('sample_mflix', 'movies')
    create_vector_index('sample_mflix', 'embedded_movies', 'fullplot_embedding', 'fullplot_embed_cosine')


def main():
    # main function
    st.title('Movie Recommendation System')
    movie_name = st.text_input('Enter a movie name', value='The Dark Knight')
    print("Movie name", movie_name)
    if st.button('Submit'):
        movies = recommend_movies(movie_name)
        # movies = search_movie(movie_name)
        print(movies)
        st.write(movies)
    # print(movies)

    pass

if __name__ == '__main__':
    # main function
    main()