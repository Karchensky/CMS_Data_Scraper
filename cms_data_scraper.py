import requests
import json
import pandas as pd
from datetime import datetime

# CMS data - supply a UUID & this will scrape the data from the endpoint here.
class cms_data_scraper:
    def __init__(self, endpoint_extention):
        self.endpoint_extention = endpoint_extention
        self.endpoint = 'https://data.cms.gov/data-api/v1/dataset/' + endpoint_extention + '/data'

    # This scrapes the CMS data for a given month
    def scraper_cms(self):
        endpoint = self.endpoint
        response = requests.get(endpoint)
        data = json.loads(response.text)
        cms_data = []
        for item in data:
            uuid = self.endpoint_extention
            provnum = item.get("PROVNUM", None)
            provname = item.get("PROVNAME", None)
            city = item.get("CITY", None)
            state = item.get("STATE", None)
            county_name = item.get("COUNTY_NAME", None)
            county_fips = item.get("COUNTY_FIPS", None)
            cy_qtr = item.get("CY_Qtr", None)
            workdate_str = item.get("WorkDate", None)
            if workdate_str is not None:
                workdate = datetime.strptime(workdate_str, "%Y%m%d").date().strftime("%Y-%m-%d")
            else:
                workdate = None
            mdscensus = item.get("MDScensus", None)
            hrs_rndon = item.get("Hrs_RNDON", None)
            hrs_rndon_emp = item.get("Hrs_RNDON_emp", None)
            hrs_rndon_ctr = item.get("Hrs_RNDON_ctr", None)
            hrs_rnadmin = item.get("Hrs_RNadmin", None)
            hrs_rnadmin_emp = item.get("Hrs_RNadmin_emp", None)
            hrs_rnadmin_ctr = item.get("Hrs_RNadmin_ctr", None)
            hrs_rn = item.get("Hrs_RN", None)
            hrs_rn_emp = item.get("Hrs_RN_emp", None)
            hrs_rn_ctr = item.get("Hrs_RN_ctr", None)
            hrs_lpnadmin = item.get("Hrs_LPNadmin", None)
            hrs_lpnadmin_emp = item.get("Hrs_LPNadmin_emp", None)
            hrs_lpnadmin_ctr = item.get("Hrs_LPNadmin_ctr", None)
            hrs_lpn = item.get("Hrs_LPN", None)
            hrs_lpn_emp = item.get("Hrs_LPN_emp", None)
            hrs_lpn_ctr = item.get("Hrs_LPN_ctr", None)
            hrs_cna = item.get("Hrs_CNA", None)
            hrs_cna_emp = item.get("Hrs_CNA_emp", None)
            hrs_cna_ctr = item.get("Hrs_CNA_ctr", None)
            hrs_natrn = item.get("Hrs_NAtrn", None)
            hrs_natrn_emp = item.get("Hrs_NAtrn_emp", None)
            hrs_natrn_ctr = item.get("Hrs_NAtrn_ctr", None)
            hrs_medaide = item.get("Hrs_MedAide", None)
            hrs_medaide_emp = item.get("Hrs_MedAide_emp", None)
            hrs_medaide_ctr = item.get("Hrs_MedAide_ctr", None)

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