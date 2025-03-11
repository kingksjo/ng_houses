import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import os
from dotenv import load_dotenv
import random
import time
from aiohttp import ClientSession, TCPConnector
import logging
from aiohttp.client_exceptions import ClientResponseError, ClientConnectorError, ServerDisconnectedError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# List of common user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
]

async def fetch_page(session, url, retries=3, backoff_factor=0.5):
    """
    Fetches the HTML content of a page asynchronously with retry logic.
    
    Args:
        session: aiohttp ClientSession
        url: URL to fetch
        retries: Number of retry attempts
        backoff_factor: Exponential backoff factor
        
    Returns:
        HTML content or None if all retries fail
    """
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': url.split('?')[0],  # Base URL as referer
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    for attempt in range(retries):
        try:
            # Add random delay between requests (between 1-3 seconds)
            await asyncio.sleep(random.uniform(1, 3))
            
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited on {url}. Waiting {retry_after} seconds.")
                    await asyncio.sleep(retry_after)
                    continue
                else:
                    logger.warning(f"HTTP {response.status} for {url} on attempt {attempt+1}")
                    
        except (ClientResponseError, ClientConnectorError, ServerDisconnectedError, asyncio.TimeoutError) as e:
            wait_time = backoff_factor * (2 ** attempt)
            logger.warning(f"Error fetching {url} (attempt {attempt+1}/{retries}): {e}. Retrying in {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None
    
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None

async def scrape_listing(listing):
    """
    Extracts data from a single property listing with improved error handling.
    
    Args:
        listing: BeautifulSoup object for a single listing
        
    Returns:
        Dictionary of property details or None if extraction fails
    """
    try:
        # Extract house type with fallback
        house_type_elem = listing.find("h4", class_="content-title")
        house_type = house_type_elem.text.strip() if house_type_elem else "Unknown"
        
        # Extract location with fallback
        location_elem = listing.find("address")
        location = location_elem.text.strip() if location_elem else "Unknown"
        
        # Extract price with fallback
        price_elements = listing.find_all("span", class_="price")
        price = "".join(p.text.strip() for p in price_elements) if price_elements else "Unknown"
        
        # Extract features with proper null handling
        features = []
        features_elem = listing.find("ul", class_="aux-info")
        if features_elem:
            features = features_elem.find_all("li")
        
        # Initialize property features
        bedrooms, bathrooms, toilets, parking_space = None, None, None, None
        
        # Extract features safely
        for feature in features:
            feature_text = feature.text.strip().lower()
            digits = "".join(filter(str.isdigit, feature_text))
            
            # Only convert to int if digits exist
            if "bedroom" in feature_text and digits:
                bedrooms = int(digits)
            elif "bathroom" in feature_text and digits:
                bathrooms = int(digits)
            elif "toilet" in feature_text and digits:
                toilets = int(digits)
            elif "parking" in feature_text and digits:
                parking_space = int(digits)
        
        return {
            "type_of_house": house_type,
            "location": location,
            "price": price,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "toilets": toilets,
            "parking_space": parking_space,
        }
    except AttributeError as e:
        logger.error(f"Error extracting data from listing: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing listing: {e}")
        return None

async def scrape_page(session, url, all_properties):
    """
    Scrapes a single page of listings with improved parsing.
    
    Args:
        session: aiohttp ClientSession
        url: URL to scrape
        all_properties: List to append results to
    """
    html = await fetch_page(session, url)
    if not html:
        logger.error(f"Failed to get HTML content for {url}")
        return 0
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # Try different selectors if the original one doesn't work
        property_listings = soup.find_all("div", class_="row property-list")
        
        # Fallback selectors if the primary one doesn't find anything
        if not property_listings:
            property_listings = soup.find_all("div", class_="property-list")
        
        if not property_listings:
            property_listings = soup.select(".property-list, .listing, .property-card")
        
        if not property_listings:
            logger.warning(f"No property listings found on {url}")
            return 0
        
        # Process listings one by one instead of concurrently to avoid overloading
        count = 0
        for listing in property_listings:
            result = await scrape_listing(listing)
            if result:
                all_properties.append(result)
                count += 1
                
        logger.info(f"Scraped {count} properties from {url}")
        return count
    except Exception as e:
        logger.error(f"Error parsing page {url}: {e}")
        return 0

async def scrape_real_estate_properties(base_url, num_pages, max_concurrent=5, save_frequency=10):
    """
    Scrapes property data from Real Estate Website asynchronously with concurrency control.
    
    Args:
        base_url: Base URL to scrape
        num_pages: Number of pages to scrape
        max_concurrent: Maximum number of concurrent requests
        save_frequency: How often to save interim results (in pages)
        
    Returns:
        DataFrame with property data
    """
    all_properties = []
    
    # Use a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Use TCPConnector with limit and longer timeout
    connector = TCPConnector(limit=max_concurrent, ttl_dns_cache=300)
    
    # Configure client session with longer timeout and persistent connections
    async with ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=60)) as session:
        # Create task chunks instead of all at once
        total_processed = 0
        
        for chunk_start in range(1, num_pages + 1, max_concurrent):
            chunk_end = min(chunk_start + max_concurrent - 1, num_pages)
            logger.info(f"Processing pages {chunk_start} to {chunk_end}")
            
            # Process a chunk of pages
            tasks = []
            for page_num in range(chunk_start, chunk_end + 1):
                # Use semaphore to limit concurrency
                async with semaphore:
                    url = f"{base_url}?page={page_num}"
                    tasks.append(scrape_page(session, url, all_properties))
            
            # Execute chunk tasks
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful pages
            successful_pages = sum(1 for r in results if isinstance(r, int) and r > 0)
            total_processed += successful_pages
            
            logger.info(f"Chunk complete. Successfully processed {successful_pages}/{len(tasks)} pages.")
            
            # Save interim results periodically
            if chunk_end % save_frequency == 0 or chunk_end == num_pages:
                interim_df = pd.DataFrame(all_properties)
                interim_df.to_csv(f"properties_interim_{chunk_end}.csv", index=False)
                logger.info(f"Saved interim results with {len(all_properties)} properties to CSV")
            
            # Add a delay between chunks to avoid overloading the server
            await asyncio.sleep(random.uniform(3, 5))
    
    logger.info(f"Scraping complete. Processed {total_processed}/{num_pages} pages successfully.")
    return pd.DataFrame(all_properties)

async def main():
    """Main function to orchestrate scraping with improved error handling and progress tracking."""
    URL = os.getenv("URL")
    
    if not URL:
        logger.error("URL not found in .env file. Please set the URL environment variable.")
        return
    
    # Get total pages from command line or use default
    try:
        total_pages = int(os.getenv("TOTAL_PAGES", "4125"))
        max_concurrent = int(os.getenv("MAX_CONCURRENT", "5"))
        save_frequency = int(os.getenv("SAVE_FREQUENCY", "10"))
    except ValueError as e:
        logger.error(f"Invalid environment variable: {e}")
        return
    
    logger.info(f"Starting scraper for {URL} with {total_pages} pages")
    logger.info(f"Using max concurrency: {max_concurrent}, saving every {save_frequency} pages")
    
    start_time = time.time()
    
    try:
        # Create results directory
        os.makedirs("results", exist_ok=True)
        
        # Run the scraper
        properties_df = await scrape_real_estate_properties(
            URL, 
            total_pages, 
            max_concurrent=max_concurrent,
            save_frequency=save_frequency
        )
        
        # Save final results
        if not properties_df.empty:
            # Clean data before saving
            properties_df = properties_df.dropna(how='all')
            
            # Save as CSV and Excel
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            csv_path = f"results/properties_{timestamp}.csv"
            excel_path = f"results/properties_{timestamp}.xlsx"
            
            properties_df.to_csv(csv_path, index=False)
            properties_df.to_excel(excel_path, index=False)
            
            logger.info(f"Saved {len(properties_df)} properties to {csv_path} and {excel_path}")
            logger.info(f"Columns: {', '.join(properties_df.columns)}")
            logger.info(f"Data sample:\n{properties_df.head()}")
        else:
            logger.error("No properties were scraped successfully")
        
    except Exception as e:
        logger.error(f"An error occurred during scraping: {e}", exc_info=True)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Scraping completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    # Set a lower event loop policy on Windows to avoid issues
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())