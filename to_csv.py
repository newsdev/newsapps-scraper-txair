import csv
import glob
import json

regions = glob.glob('data/*.json')

contaminants_out = []
events_out = []

for region in regions:
    region_name = region.split('/')[-1].split('.json')[0].replace('-', ' ')
    with open(region, 'r') as readfile:
        events = [v for k,v in json.loads(readfile.read()).items()]
        for event in events:
            for source in event['sources']:
                if len(source['contaminants']) > 0:
                    contaminants = list([c for c in source['contaminants']])
                    del source['contaminants']
                    for c in contaminants:
                        for k,v in source.items():
                            key_name = "source_%s" % k
                            c[k] = v
                        c['event_id'] = event['id']
                        c['region'] = region_name 
                        contaminants_out.append(c)

            del event['sources']
            event['region'] = region_name
            events_out.append(event)

with open('data/events.csv', 'w') as writefile:
    fieldnames = events_out[0].keys()
    writer = csv.DictWriter(writefile, fieldnames=fieldnames)
    writer.writeheader()
    for e in events_out:
        writer.writerow(e)

with open('data/contaminants.csv', 'w') as writefile:
    fieldnames = contaminants_out[0].keys()
    writer = csv.DictWriter(writefile, fieldnames=fieldnames)
    writer.writeheader()
    for e in contaminants_out:
        writer.writerow(e)