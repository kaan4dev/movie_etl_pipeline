import requests
import pandas as pd
import os

API_KEY = "593682f76ba4b7e789eb5c52e7775f37"
BASE_URL = "https://api.themoviedb.org/3"

def fetch_popular_movies(pages = 500):
    all_movies = []
    for page in range(1, pages + 1):
        url = f"{BASE_URL}/movie/popular"
        params = {"api_key": API_KEY, "language": "en-US", "page": page}
        response = requests.get(url, params=params)
        data = response.json()

        results = data.get("results", [])
        if not results:  
            break    

        all_movies.extend(results)
        print(f"Page {page} extracted, total {len(all_movies)} movies")
                
    return all_movies


def main():
    movies = fetch_popular_movies(pages= 500)
    df = pd.DataFrame(movies)

    os.makedirs("../data/raw", exist_ok=True)
    df.to_csv("../data/raw/movies_tmdb.csv", index=False)
    print("API data saved: data/raw/movies_tmdb.csv")

if __name__ == "__main__":
    main()
