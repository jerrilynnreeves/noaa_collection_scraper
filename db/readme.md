Files in this directory initiize and seed a light weight database used to increase the effienceny, speed, and management of etags scrape from the collection manager waf. 

# Intial Setup
From the root directory run
python -m noaa_collection_scraper.init.db_creation

# Load Database dump
@ todo - create script to load etag_dump gz

# Create a new database dump file compressed
compressing the backup means it goes from 21 MB to about 2
python -m noaa_collection_scraper.init.db_dump