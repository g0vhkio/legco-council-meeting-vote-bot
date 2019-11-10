import requests
import sys
from lxml import etree
import re
import os
from datetime import date, datetime
import scraperwiki
from slackclient import SlackClient
from os import environ
import hashlib
import simplejson as json
from hashlib import md5
from lxml.html.clean import Cleaner
import json


def upload_vote(vote_json, legco_api_token):
    headers = {'Content-Type': 'application/json', 'Authorization': 'Token ' + legco_api_token}
    r = requests.put("https://api.g0vhk.io/legco/upsert_vote/", json=vote_json, headers=headers)
    try:
        j = r.json()
    except Exception as e:
        print(r.text)
        raise e
    return j


def crawl(token, channel, legco_api_token, year):
    if year == 0:
        today = date.today()
        year = today.year
        if today.month < 10:
            year = year - 1
    current_year = year - 2000
    year_range = "%d-%d" % (current_year, current_year + 1) 
    meeting_types = ["cm", "esc", "pwsc", "hc", "fc"]
    url_format = {
        "cm": "http://www.legco.gov.hk/yr%s/chinese/counmtg/voting/cm_vote_",
        "esc": "http://www.legco.gov.hk/yr%s/chinese/fc/esc/results/esc_vote_",
        "pwsc": "http://www.legco.gov.hk/yr%s/chinese/fc/pwsc/results/pwsc_vote_",
        "hc": "http://www.legco.gov.hk/yr%s/chinese/hc/voting/hc_vote_",
        "fc": "http://www.legco.gov.hk/yr%s/chinese/fc/fc/results/fc_vote_"
    }
    detect_url_format = \
        "http://www.legco.gov.hk/php/detect-votes.php?term=yr%s&meeting=%s"

    slack = SlackClient(token)
    for yr in [year_range]:
        for mc in meeting_types:
            detect_url = detect_url_format % (yr, mc)
            r = requests.get(detect_url)
            print(detect_url)
            xml_files = [f for f in r.text.split(",") if f.endswith(".xml")]
            for xml_file in xml_files:
                download_url = url_format[mc] % (yr) + xml_file
                key = download_url
                existed = False
                try:
                    existed = len(scraperwiki.sqlite.select('* from swdata where key = "%s"' % key)) > 0
                except:
                    pass
                if existed:
                    print('%s already sccraped' % key)
                    continue
                vote_json = {'url': download_url}
                results = upload_vote(vote_json, legco_api_token)
                for result in results:
                    created = result['created']
                    vote_date = result['meeting']['date']
                    votes = len(result['votes'])
                    scraperwiki.sqlite.save(unique_keys=['key'], data={'key': key, 'meeting_type': mc, 'date': vote_date})
                    if created:
                        text = "%d votes on %s available at %s." % (votes, vote_date, key)
                        if channel:
                            slack.api_call(
                                    "chat.postMessage",
                                    channel=channel,
                                    text=text
                            )
                        print(text)
                        return
                    else:
                        print('%s already loaded to API server.' % key )

TOKEN = environ.get('MORPH_TOKEN', None)
CHANNEL = environ.get('MORPH_CHANNEL', None)
LEGCO_API_TOKEN = environ.get('MORPH_LEGCO_API_TOKEN', None)
YEAR = int(environ.get('MORPH_YEAR', '0'))
crawl(TOKEN, CHANNEL, LEGCO_API_TOKEN, YEAR)
