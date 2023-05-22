import requests
import json
import pandas as pd
from datetime import datetime

# CMS data - supply a UUID & this will scrape the data from the endpoint here.
class cms_data_scraper:
    def __init__(self, endpoint_extention):
        self.endpoint_extention = endpoint_extention
        self.endpoint = 'https://data.cms.gov/data-api/v1/dataset/' + endpoint_extention + '/data'

    # Handles mismatched upper/lower case fields
    def get_key(self, item, key):
        return item.get(key.upper(), item.get(key.lower(), None))

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
            workdate_str = self.get_key(item, "WorkDate")
            if workdate_str is not None:
                workdate = datetime.strptime(workdate_str, "%Y%m%d").date().strftime("%Y-%m-%d")
            else:
                workdate = None
            mdscensus = self.get_key(item, "MDScensus")
            hrs_rndon = self.get_key(item, "Hrs_RNDON")
            hrs_rndon_emp = self.get_key(item, "Hrs_RNDON_emp")
            hrs_rndon_ctr = self.get_key(item, "Hrs_RNDON_ctr")
            hrs_rnadmin = self.get_key(item, "Hrs_RNadmin")
            hrs_rnadmin_emp = self.get_key(item, "Hrs_RNadmin_emp")
            hrs_rnadmin_ctr = self.get_key(item, "Hrs_RNadmin_ctr")
            hrs_rn = self.get_key(item, "Hrs_RN")
            hrs_rn_emp = self.get_key(item, "Hrs_RN_emp")
            hrs_rn_ctr = self.get_key(item, "Hrs_RN_ctr")
            hrs_lpnadmin = self.get_key(item, "Hrs_LPNadmin")
            hrs_lpnadmin_emp = self.get_key(item, "Hrs_LPNadmin_emp")
            hrs_lpnadmin_ctr = self.get_key(item, "Hrs_LPNadmin_ctr")
            hrs_lpn = self.get_key(item, "Hrs_LPN")
            hrs_lpn_emp = self.get_key(item, "Hrs_LPN_emp")
            hrs_lpn_ctr = self.get_key(item, "Hrs_LPN_ctr")
            hrs_cna = self.get_key(item, "Hrs_CNA")
            hrs_cna_emp = self.get_key(item, "Hrs_CNA_emp")
            hrs_cna_ctr = self.get_key(item, "Hrs_CNA_ctr")
            hrs_natrn = self.get_key(item, "Hrs_NAtrn")
            hrs_natrn_emp = self.get_key(item, "Hrs_NAtrn_emp")
            hrs_natrn_ctr = self.get_key(item, "Hrs_NAtrn_ctr")
            hrs_medaide = self.get_key(item, "Hrs_MedAide")
            hrs_medaide_emp = self.get_key(item, "Hrs_MedAide_emp")
            hrs_medaide_ctr = self.get_key(item, "Hrs_MedAide_ctr")

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