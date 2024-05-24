from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
import logging, os
import json, csv
from dataclasses import dataclass, field, fields, asdict
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPTIONS = ChromeOptions()
OPTIONS.add_argument("--headless")

API_KEY = "YOUR-SUPER-SECRET-API-KEY"

def search_products(product_name: str, retries=3):
    tries = 0
    success = False

    while tries < retries and not success:
        try:
            driver = webdriver.Chrome(options=OPTIONS)
            url = f"https://www.amazon.com/s?k={product_name}"
            driver.get(url)

            logger.info("Successfully fetched page")                
            
            #remove the bad divs
            bad_divs = driver.find_elements(By.CSS_SELECTOR, "div.AdHolder")
            
            for bad_div in bad_divs:
                driver.execute_script("""
                    var element = arguments[0];
                    element.parentNode.removeChild(element);
                """, bad_div)
            #find the regular divs
            divs = driver.find_elements(By.TAG_NAME, "div")
            #copy them to help with stale elements
            copied_divs = divs
            last_title = ""

            for div in copied_divs:
                h2s = div.find_elements(By.TAG_NAME, "h2")
                
                parsable = len(h2s) > 0
                if parsable:
                    h2 = div.find_element(By.TAG_NAME, "h2")
                
                if h2 and parsable:
                    title = h2.text

                    if title == last_title:
                        continue

                    a = h2.find_element(By.TAG_NAME, "a")
                        
                    product_url = (a.get_attribute("href") if a else "").replace("proxy.scrapeops.io", "www.amazon.com")

                    ad_status = False
                    if "sspa" in product_url:
                        ad_status = True

                    url_array = product_url.split("/")
                    asin = url_array[5]

                    price_symbols_array = div.find_elements(By.CSS_SELECTOR, "span.a-price-symbol")
                    has_price = len(price_symbols_array) > 0

                    if not has_price:
                        continue

                    symbol_element = div.find_element(By.CSS_SELECTOR, "span.a-price-symbol")

                    pricing_unit = symbol_element.text
                        
                    price_whole = div.find_element(By.CSS_SELECTOR, "span.a-price-whole")
                    price_decimal = div.find_element(By.CSS_SELECTOR, "span.a-price-fraction")                        
                    price_str = f"{price_whole.text}.{price_decimal.text}"
                        
                    rating_element = div.find_element(By.CLASS_NAME, "a-icon-alt")
                    rating = rating_element.get_attribute("innerHTML")


                    price = float(price_str)
                    real_price_array = div.find_elements(By.CSS_SELECTOR, "span.a-price.a-text-price")

        
                    real_price = 0.0                        
                    if len(real_price_array) > 0:
                        real_price_str = real_price_array[0].text.replace(pricing_unit, "")
                        real_price = float(real_price_str)
                    else:
                        real_price = price

                    product = {
                        "name": asin,
                        "title": title,
                        "url": product_url,
                        "is_ad": ad_status,
                        "pricing_unit": pricing_unit,
                        "price": price,
                        "real_price": real_price,
                        "rating": rating
                    }
                    
                    print(product)

                    last_title = title

                else:
                    continue
            success = True

            if not success:        
                raise Exception(f"Failed to scrape the page {page_number}, tries left: {retries-tries}")
            
    
        except Exception as e:
            logger.warning(f"Failed to scrape page, {e}")
            tries += 1

        finally:    
            driver.quit()
    
        
    if not success:
        logger.warning(f"Failed to scrape page, retries exceeded: {retries}")



if __name__ == "__main__":

    PRODUCTS = ["phone"]
    MAX_RETRIES = 2
    
    for product in PRODUCTS:
        search_products(product)