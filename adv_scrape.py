import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

async def fetch_page(session, url):
    """Fetches the HTML content of a page asynchronously."""
    try:
        async with session.get(url) as response:
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return await response.text()
    except aiohttp.ClientError as e:
        print(f"Error fetching {url}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching {url}: {e}")
        return None

async def scrape_listing(listing):
    """Extracts data from a single property listing."""
    try:
        house_type = listing.find("h4", class_="content-title").text.strip()
        location = listing.find("address").text.strip()
        price_elements = listing.find_all("span", class_="price")
        price = "".join(p.text.strip() for p in price_elements)
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

        return {
            "type of house": house_type,
            "location": location,
            "price": price,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "toilets": toilets,
            "parking_space": parking_space,
        }
    except AttributeError as e:
        print(f"Error extracting data from listing: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error processing listing: {e}")
        return None

async def scrape_page(session, url, all_properties):
    """Scrapes a single page of listings."""
    html = await fetch_page(session, url)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        property_listings = soup.find_all("div", class_="row property-list")

        tasks = [scrape_listing(listing) for listing in property_listings]
        results = await asyncio.gather(*tasks)

        for result in results:
            if result:
                all_properties.append(result)
        print(f"Scraped {url}")

async def scrape_real_estate_properties(base_url, num_pages):
    """Scrapes property data from Real Estate Website asynchronously."""
    all_properties = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for page_num in range(1, num_pages + 1):
            url = f"{base_url}?page={page_num}"
            tasks.append(scrape_page(session, url, all_properties))
        await asyncio.gather(*tasks)
    return pd.DataFrame(all_properties)

async def main():
    """Main function to orchestrate scraping."""
    URL = os.getenv("URL")
    num_pages = 4125
    if URL:
        try:
            properties_df = await scrape_real_estate_properties(URL, num_pages)
            print(properties_df)
            properties_df.to_csv("properties_async.csv", index=False)
        except Exception as e:
            print(f"An error occurred during scraping: {e}")
    else:
        print("URL not found in .env file. Please set the URL environment variable.")

if __name__ == "__main__":
    asyncio.run(main())