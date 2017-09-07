import csv
import json
import os

from bs4 import BeautifulSoup
import requests

import utils


BASE_URL = "http://www2.tceq.texas.gov/oce/eer/index.cfm?fuseaction=main.dispatchSearch&startmonth=8&startday=01&startyear=2017&endmonth=&endday=&endyear=&county=&ctyorreg=region&region=%s&doit=Submit"
DETAIL_URL = "http://www2.tceq.texas.gov/oce/eer/index.cfm?fuseaction=main.getDetails&target=%s"
DATA_DIR = os.environ.get('DATA_DIR', 'data/')
REGIONS = ["10", "12"]

class Scrape:
    """
    A class to hold state for this scraper.
    Gotta have state, dawg.
    """
    new_incident_ids = [] # Just doing this for testing.
    old_incident_ids = []
    incidents = {}
    region = None

    def __init__(self, *args, **kwargs):
        """
        This is run when the class instantiates.
        It's the control flow for our scrape.
        """
        self.region = kwargs.get('region', None)
        self.retrieve_old_incidents()
        self.scrape_new_incidents() # Just doing this for testing.
        self.scrape_new_details()
        self.persist_incidents()

    def retrieve_old_incidents(self):
        """
        Grabs old incidents from our Google Cloud persistence.
        Loads them up into an incidents dict stored in the Class.
        """
        self.incidents = {} # Empty for now. Solving this problem later.

    def scrape_new_incidents(self):
        """
        Gets a list of IDs for incidents.
        Only stores IDs for ones we haven't seen before.
        """
        r = requests.get(BASE_URL % self.region)
        soup = BeautifulSoup(r.text, 'lxml')
        rows = soup.select('div#content > table tr')[1:]
        incidents = {}

        for row in rows:
            cells = row.select('td')
            incident_id = cells[0].text.strip()

            if not self.incidents.get(incident_id, None):
                self.new_incident_ids.append(incident_id)
    
    def scrape_new_details(self):
        """
        Composable method for scraping new events.
        """

        # Only continue if we have some new incidents.
        if len(self.new_incident_ids) > 0:

            # Loop over every incident id.
            for idnum in set(self.new_incident_ids):


                print(idnum)

                # Fetch the detail html.
                r = requests.get(DETAIL_URL % idnum)
                soup = BeautifulSoup(r.text, 'lxml')

                # There are 1+n tables on this page.
                # Table 1 is a summary of the details.
                # Table 1+n are the sources of emissions and possible totals.
                # There are h3 tags for each source.
                # Sometimes there are tables beneath the tags.
                # Sometimes there are not.
                tables = soup.select('div#content table')
                heds = soup.select('div#content h3')

                # Set up a data dictionary for this event.
                data_dict = {'id': idnum, 'sources': []}

                # Still figuring out how to get the source / totals.
                # For now, let's get the summary data.
                first_table_rows = tables[0].select('tr')

                # Models the table pattern visible here: http://www2.tceq.texas.gov/oce/eer/index.cfm?fuseaction=main.getDetails&target=266558
                # This looks crazy.
                # Is "none" for cells to skip; "name to use" for cells to grab.
                # Uses the index of the list to id the cell.
                first_table_pattern = [
                    [None, "name", None, "location"],
                    [None, "rn_number", None, "city_and_county"],
                    [None, "event_type", None, "start_date"],
                    [None, "report_type", None, "end_date"],
                    [None, "cause"],
                    [None, "action"],
                    [None, "estimation_method"]
                ]

                # Loop over the rows in the first table.
                for idx, row in enumerate(first_table_rows):

                    # Since the cells alternate td and th, we can't rely on the parser to so .select() or .find().
                    # So I just grab the children and strip out the \n and \r.
                    cells = [c for c in first_table_rows[idx].children if c.string != "\n"]

                    # Okay, loop over the rows in the pattern.
                    # For the position, check if I have a name for it.
                    # If no name, skip.
                    # If there's a name, grab the same-indexed cell from the table HTML.
                    for pidx, pattern in enumerate(first_table_pattern[idx]):
                        if pattern:
                            try:
                                data_dict[pattern] = " ".join(cells[pidx].text.strip().split())
                            except:
                                data_dict[pattern] = None

                #
                # Loop over the tables at the bottom of the list.
                # Knit the H3s with the tables they go with.
                # 
                num_other_tables = len(tables) - 1
                num_headers = len(heds)
                content_items = [c for c in soup.select('div#content')[0].children if c != "\n"]

                # Two things we need to store:
                # The previous item (possibly the source h3)
                # Our current list of sources, keyed by the h3 name.
                previous_item = None
                sources = {}

                # Loop over all the content items in div#content skipping the first table
                for c in content_items[1:]:
                    # Only do our business over h3 and tables, since h3 are the source names and the tables have the data.
                    if c.name in ['h3', 'table']:

                        if c.name == 'h3':
                            sources[" ".join(c.text.strip().split())] = {"source": " ".join(c.text.strip().split()), "contaminants": []}                            

                        if c.name == 'table':
                            if previous_item:
                                if previous_item.name == "h3":

                                    # Loop over the table rows, skipping the header row.
                                    for row in c.select('tr')[1:]:
                                        chem_dict = {}
                                        cells = row.select('td')
                                        chem_dict['contaminant'] = " ".join(cells[0].text.strip().split())
                                        chem_dict['authorization'] = " ".join(cells[1].text.strip().split())
                                        chem_dict['limit'] = " ".join(cells[2].text.strip().split())
                                        chem_dict[c.select('tr')[0].select('th')[3].text.strip().lower().replace(' ', '_')] = " ".join(cells[3].text.strip().split())
                                        sources[" ".join(previous_item.text.strip().split())]['contaminants'].append(chem_dict)
                            
                        previous_item = c

                data_dict['sources'] = [v for k,v in sources.items()]    

                # Load up our event data into the class's incidents dictionary.
                # Uses the event id as a key so we don't get duplicates.
                self.incidents[idnum] = data_dict

    def persist_incidents(self):
        with open('data/region-%s.json' % self.region, 'w') as writefile:
            writefile.write(json.dumps(self.incidents))

if __name__ == "__main__":
    for region in REGIONS:
        s = Scrape(region=region)