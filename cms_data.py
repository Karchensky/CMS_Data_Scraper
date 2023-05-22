from cms_data_scraper import cms_data_scraper
from db_manager import db_manager

# Create an instance of the db_manager & specify where you would like the data saved
database = db_manager(r'D:\Scripts\cms_data\cms_data\cms_data.db')

# Create the stg_cms_data table in your database
database.create_table_stg_cms_data()

# Specify the list of UUID's you would like scraped. 
uuid_list = [
    '7a678e92-0418-44e0-9d72-043fd23f3959',
    'd6d70d6c-776c-43e6-82e3-e84637adf6e8',
    'dd928f23-3bd4-46ec-99e2-7463db232b47',
    '1fcb1016-612d-4605-8296-6b220d20e851',
    '93881636-0cde-4118-a110-ba2fd8cc890a',
    '050ddc4f-9214-42d3-8c32-3446a746e8cd',
    'd10d792e-ea6e-4145-8512-34efbc1be04b',
    '0a33dac8-73b2-4223-83e7-d709645c3549',
    '864a622a-733a-4199-a994-bc10ae071cb0',
    '30ffaf4e-a749-4bdb-bb3e-6c0b28fe649b',
    '1f6bc389-d388-4746-9b1a-f642803863e1',
    'c58e3bc9-a27c-4540-aec7-24153fc5b115',
    'c90dd8a4-8f97-4b56-b756-4ec09c9bb034',
    'd8e71c8a-19f2-484e-951b-deba329e7197',
    'f92c9237-d7d7-4747-b1c8-e8070d044bcc',
    'a1ab6217-d261-4f3c-b1c7-d5ac29d2628f',
    'b63f6abb-dc58-48ca-98a1-7081c485b39e',
    'ef50b53a-6f1a-4d0e-9322-ed05fd48adc4',
    '77a7052f-e0b0-4735-bc38-912b79c67c5f',
    'e07ea294-06eb-4a9b-9d01-a6631cd1d630',
    '88f0d985-21b8-4c07-899d-f0c7600d6aaa',
    'ef33699d-8891-4943-89e7-7db02b43613e',
    '2d40353c-3ea1-4200-9e88-630b9a703a05',
    '9e445238-71ea-4101-a560-56b4b7871f8a'
    ]

# Loop through UUID list - 1) scrape the data, 2) upload to STG_CMS_DATA table we created earlier
for uuid in uuid_list:
    data_scraper = cms_data_scraper(uuid)
    cms_data = data_scraper.scraper_cms()
    database.db_append("STG_CMS_DATA", cms_data)