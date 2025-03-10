import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def scrape_real_estate_properties(base_url, num_pages):
    """Scrapes property data from Real Estate Website."""

    all_properties = []

    for page_num in range(1, num_pages + 1):
        url = f"{base_url}?page={page_num}"
        response = requests.get(url)

        if response.status_code == 200:
            
            soup = BeautifulSoup(response.content, "html.parser")
            property_listings = soup.find_all("div", class_="row property-list")
            print(f"Scraping page {page_num} ...")
            for listing in property_listings:
                try:
                    # Extract type of house
                    house_type = listing.find("h4", class_="content-title").text.strip()

                    # Extract location
                    location = listing.find("address").text.strip()

                    # Extract price
                    price_elements = listing.find_all("span", class_="price")
                    price = "".join(p.text.strip() for p in price_elements)

                    # Extract features
                    features = listing.find("ul", class_="aux-info").find_all("li")
                    bedrooms, bathrooms, toilets, parking_space = None, None, None, None

                    for feature in features:
                        feature_text = feature.text.strip().lower()
                        if "bedroom" in feature_text:
                            bedrooms = int("".join(filter(str.isdigit, feature_text)))
                        elif "bathroom" in feature_text:
                            bathrooms = int("".join(filter(str.isdigit, feature_text)))
                        elif "toilet" in feature_text:
                            toilets = int("".join(filter(str.isdigit, feature_text)))
                        elif "parking" in feature_text:
                            parking_space = int("".join(filter(str.isdigit, feature_text)))

                    all_properties.append({
                        "type of house": house_type,
                        "location": location,
                        "price": price,
                        "bedrooms": bedrooms,
                        "bathrooms": bathrooms,
                        "toilets": toilets,
                        "parking_space": parking_space,
                    })
                
                except AttributeError as e:
                    print(f"Error extracting data: {e}")
                    continue #skip to the next listing.

        else:
            print(f"Failed to fetch page {page_num}: {response.status_code}")

    df = pd.DataFrame(all_properties)
    return df

# Usage
URL = os.getenv("URL")
num_pages = 4 # the number of pages to scrape.
properties_df = scrape_real_estate_properties(URL, num_pages)
print(properties_df)
properties_df.to_csv("properties.csv", index=False) #save to csv.