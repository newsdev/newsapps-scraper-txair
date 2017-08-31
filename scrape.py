import csv
import json
import os

from bs4 import BeautifulSoup
import requests

import utils


BASE_URL = "http://www2.tceq.texas.gov/oce/eer/index.cfm?fuseaction=main.dispatchSearch&startmonth=8&startday=01&startyear=2017&endmonth=&endday=&endyear=&county=&ctyorreg=region&region=12&doit=Submit"
DETAIL_URL = "http://www2.tceq.texas.gov/oce/eer/index.cfm?fuseaction=main.getDetails&target=%s"
DATA_DIR = os.environ.get('DATA_DIR', 'data/')


class Scrape:
    new_incident_ids = ['266556', '266558']
    old_incident_ids = []
    incidents = {}

    def __init__(self):
        self.retrieve_old_incidents()
        # self.scrape_new_incidents()
        self.scrape_new_details()
        self.persist_incidents()

    def retrieve_old_incidents(self):
        self.incidents = {}

    def scrape_new_incidents(self):
        r = requests.get(BASE_URL)
        soup = BeautifulSoup(r.text, 'lxml')
        rows = soup.select('div#content > table tr')[1:]
        incidents = {}

        for row in rows:
            cells = row.select('td')
            incident_id = cells[0].text.strip()

            if not self.incidents.get(incident_id, None):
                self.new_incident_ids.append(incident_id)
    
    def scrape_new_details(self):
        if len(self.new_incident_ids) > 0:
            for idnum in set(self.new_incident_ids):
                r = requests.get(DETAIL_URL % idnum)
                soup = BeautifulSoup(r.text, 'lxml')

                tables = soup.select('div#content table')
                heds = soup.select('div#content h3')

                data_dict = {'id': idnum}
                first_table_rows = tables[0].select('tr')

                first_table_pattern = [
                    [None, "name", None, "location"],
                    [None, "rn_number", None, "city_and_county"],
                    [None, "event_type", None, "start_date"],
                    [None, "report_type", None, "end_date"],
                    [None, "cause"],
                    [None, "action"],
                    [None, "estimation_method"]
                ]

                for idx, row in enumerate(first_table_rows):
                    cells = [c.string.strip() for c in first_table_rows[idx].children if c.string.strip() != ""]
                    for pidx, pattern in enumerate(first_table_pattern[idx]):
                        if pattern:
                            data_dict[pattern] = " ".join(cells[pidx].strip().split())

                self.incidents[idnum] = data_dict

    def persist_incidents(self):
        print(len(self.incidents.items()))

if __name__ == "__main__":
    s = Scrape()