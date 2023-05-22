import requests
import json
import pandas as pd
from datetime import datetime

# CMS data - supply a UUID & this will scrape the data from the endpoint here.
class cms_data_scraper:
    def __init__(self, endpoint_extention):
        self.endpoint_extention = endpoint_extention
        self.endpoint = 'https://data.cms.gov/data-api/v1/dataset/' + endpoint_extention + '/data'

    # Handles case-insensitive fields
    def get_key(self, item, key):
        for k in item.keys():
            if k.upper() == key.upper():
                return item[k]
        return None

    # This scrapes the CMS data for a given month
    def scraper_cms(self):
        endpoint = self.endpoint
        response = requests.get(endpoint)
        data = json.loads(response.text)
        cms_data = []
        for item in data:

            uuid = self.endpoint_extention
            provnum = self.get_key(item, "PROVNUM")
            provname = self.get_key(item, "PROVNAME")
            city = self.get_key(item, "CITY")
            state = self.get_key(item, "STATE")
            county_name = self.get_key(item, "COUNTY_NAME")
            county_fips = self.get_key(item, "COUNTY_FIPS")
            cy_qtr = self.get_key(item, "CY_QTR")
            workdate_str = self.get_key(item, "WORKDATE")
            if workdate_str is not None:
                workdate = datetime.strptime(workdate_str, "%Y%m%d").date().strftime("%Y-%m-%d")
            else:
                workdate = None
            mdscensus = self.get_key(item, "MDSCENSUS")
            hrs_rndon = self.get_key(item, "HRS_RNDON")
            hrs_rndon_emp = self.get_key(item, "HRS_RNDON_EMP")
            hrs_rndon_ctr = self.get_key(item, "HRS_RNDON_CTR")
            hrs_rnadmin = self.get_key(item, "HRS_RNADMIN")
            hrs_rnadmin_emp = self.get_key(item, "HRS_RNADMIN_EMP")
            hrs_rnadmin_ctr = self.get_key(item, "HRS_RNADMIN_CTR")
            hrs_rn = self.get_key(item, "HRS_RN")
            hrs_rn_emp = self.get_key(item, "HRS_RN_EMP")
            hrs_rn_ctr = self.get_key(item, "HRS_RN_CTR")
            hrs_lpnadmin = self.get_key(item, "HRS_LPNADMIN")
            hrs_lpnadmin_emp = self.get_key(item, "HRS_LPNADMIN_EMP")
            hrs_lpnadmin_ctr = self.get_key(item, "HRS_LPNADMIN_CTR")
            hrs_lpn = self.get_key(item, "HRS_LPN")
            hrs_lpn_emp = self.get_key(item, "HRS_LPN_EMP")
            hrs_lpn_ctr = self.get_key(item, "HRS_LPN_CTR")
            hrs_cna = self.get_key(item, "HRS_CNA")
            hrs_cna_emp = self.get_key(item, "HRS_CNA_EMP")
            hrs_cna_ctr = self.get_key(item, "HRS_CNA_CTR")
            hrs_natrn = self.get_key(item, "HRS_NATRN")
            hrs_natrn_emp = self.get_key(item, "HRS_NATRN_EMP")
            hrs_natrn_ctr = self.get_key(item, "HRS_NATRN_CTR")
            hrs_medaide = self.get_key(item, "HRS_MEDAIDE")
            hrs_medaide_emp = self.get_key(item, "HRS_MEDAIDE_EMP")
            hrs_medaide_ctr = self.get_key(item, "HRS_MEDAIDE_CTR")

            cms_data.append([
                uuid, provnum, provname, city, state, county_name, county_fips, cy_qtr,
                workdate, mdscensus, hrs_rndon, hrs_rndon_emp, hrs_rndon_ctr,
                hrs_rnadmin, hrs_rnadmin_emp, hrs_rnadmin_ctr, hrs_rn, hrs_rn_emp,
                hrs_rn_ctr, hrs_lpnadmin, hrs_lpnadmin_emp, hrs_lpnadmin_ctr, hrs_lpn,
                hrs_lpn_emp, hrs_lpn_ctr, hrs_cna, hrs_cna_emp, hrs_cna_ctr,
                hrs_natrn, hrs_natrn_emp, hrs_natrn_ctr, hrs_medaide, hrs_medaide_emp,
                hrs_medaide_ctr
            ])

        headers = [
            "UUID", "PROVNUM", "PROVNAME", "CITY", "STATE", "COUNTY_NAME", "COUNTY_FIPS",
            "CY_QTR", "WORKDATE", "MDSCENSUS", "HRS_RNDON", "HRS_RNDON_EMP",
            "HRS_RNDON_CTR", "HRS_RNADMIN", "HRS_RNADMIN_EMP", "HRS_RNADMIN_CTR",
            "HRS_RN", "HRS_RN_EMP", "HRS_RN_CTR", "HRS_LPNADMIN", "HRS_LPNADMIN_EMP",
            "HRS_LPNADMIN_CTR", "HRS_LPN", "HRS_LPN_EMP", "HRS_LPN_CTR", "HRS_CNA",
            "HRS_CNA_EMP", "HRS_CNA_CTR", "HRS_NATRN", "HRS_NATRN_EMP", "HRS_NATRN_CTR",
            "HRS_MEDAIDE", "HRS_MEDAIDE_EMP", "HRS_MEDAIDE_CTR"
        ]

        df = pd.DataFrame(cms_data, columns=headers)
        return df