import os
import re
import sys
#sys.path.append('/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages')
import threading

import pymongo
import time
import datetime
from pnmatcher import PhoneNumberMatcher
from bs4 import BeautifulSoup
matcher = PhoneNumberMatcher()



STAT_TOTAL_PROCESSED = 0
STAT_INVALID_POST_BODY = 0
STAT_INVALID_NUMBER = 0
STAT_NUMBERS_NOT_FOUND = 0
STAT_NUMBERS_FOUND = 5
STAT_PERCENT = 1


client = pymongo.MongoClient('reports.tiretracked.com', 27017)
collection_blacklist = client.get_database('cl_scraper').get_collection('blacklist')
collection_scraped = client.get_database('cl_scraper').get_collection('scraped')
db = client.database_name


def print_counts():
    print('Count of blacklisted documents {}'.format(collection_blacklist.estimated_document_count()))
    print('Count of scraped documents {}'.format(collection_scraped.estimated_document_count()))


def print_stats():
    print("STATS")
    print("\tTotal Processed: {}".format(STAT_TOTAL_PROCESSED))
    print("\tInvalid Post Body: {}".format(STAT_INVALID_POST_BODY))
    print("\tInvalid Number: {}".format(STAT_INVALID_NUMBER))
    print("\tNumbers Not Found: {}".format(STAT_NUMBERS_NOT_FOUND))
    print("\tNumbers Found: {}".format(STAT_NUMBERS_FOUND))
    print("\tFound Rate: "+" {:.3%}".format(float(STAT_NUMBERS_FOUND) / STAT_TOTAL_PROCESSED))


def get_no_number_documents():
    while True:
        queue.extend(list(collection_blacklist.find({'reason': 'no_phone_number'}).limit(100)))
        time.sleep(5)


def set_field_blacklisted_document(document, key, value):
    collection_blacklist.update_one({'_id': document['_id']}, {'$set': {key: value}})


def create_scraped_document(phone_number, ad_title, ad_url, ad_html):
    collection_scraped.insert_one({
        'phone_number': str(phone_number),
        'ad_title': str(ad_title),
        'ad_url': str(ad_url),
        'ad_html': str(ad_html),
        'scraped_at': datetime.datetime.now(),
        'is_motorcycle': ('/mcy/' in ad_url and '/cto/' not in ad_url),
        'source': os.path.basename(__file__)
    })


def get_post_body_from_document(parsed_html):
    global STAT_INVALID_POST_BODY
   # html = document['ad_html']
    #parsed_html = BeautifulSoup(html, "html.parser")

    if parsed_html is None or parsed_html.body is None:
        STAT_INVALID_POST_BODY = STAT_INVALID_POST_BODY + 1
        return None

    # Parse the body..
    if parsed_html.body.find('section', attrs={'id': 'postingbody'}) is None:
        STAT_INVALID_POST_BODY = STAT_INVALID_POST_BODY + 1
        return None

    # Clean the body..
    cleaned_html = parsed_html.body.find('section', attrs={'id': 'postingbody'}).text
    cleaned_html = cleaned_html.replace('\n', '').replace(',', '').replace('QR Code Link to This Post', '')
    return cleaned_html


def get_post_title_from_document(parsed_html):
    #html = document['ad_html']
    #parsed_html = BeautifulSoup(html, "html.parser")

    if parsed_html is None or parsed_html.body is None:
        return None

    # Parse the title..
    if parsed_html.body.find('span', attrs={'id': 'titletextonly'}) is None:
        return None

    # Clean the title..
    cleaned_html = parsed_html.body.find('span', attrs={'id': 'titletextonly'}).text
    cleaned_html = cleaned_html.replace(',', '')
    return cleaned_html

TITLE_RE = re.compile("id=\"titletextonly\">([^<]*)</span>")

def get_post_title_with_re(html):
    return TITLE_RE.search(html).group(1)


def match_number_in_html(html):
    global STAT_NUMBERS_NOT_FOUND

    # Match loose phone numbers using library..
    result = matcher.match(html, "text")
    if len(result) < 1:
        STAT_NUMBERS_NOT_FOUND = STAT_NUMBERS_NOT_FOUND + 1
        return None

    # Filter results where length is 10..
    result = filter(lambda x: len(x) == 10, result)
    if len(result) < 1:
        STAT_NUMBERS_NOT_FOUND = STAT_NUMBERS_NOT_FOUND + 1
        return None

    return result[0]


def process_numbers_in_blacklist_documents():

    global STAT_TOTAL_PROCESSED, STAT_INVALID_NUMBER, STAT_INVALID_POST_BODY, STAT_NUMBERS_FOUND

    # pool = ThreadPoolExecutor(100)
    # futures = []
    print("STarted thread")

    while True:
        while len(queue) == 0:
            continue
        document = queue.pop(0)


        soupy = BeautifulSoup(document['ad_html'], "html.parser")


        print("processing, %s left in queue" % len(queue))
        STAT_TOTAL_PROCESSED = STAT_TOTAL_PROCESSED + 1


        # Print stats every 100 processed...
        if STAT_TOTAL_PROCESSED % 100 == 0:
            print_stats()

        # Get the title for the post from html...
        post_title = get_post_title_from_document(soupy)
        #post_title = get_post_title_with_re(document['ad_html'])
        if post_title is None:
            continue


        body_html = get_post_body_from_document(soupy)
        if body_html is None:
            continue

        phone_number = match_number_in_html(body_html)
        #phone_number = match_number_in_html(document['ad_html'])
        if phone_number is None:
            continue

        STAT_NUMBERS_FOUND = STAT_NUMBERS_FOUND + 1



        # Create new document in scraped collection...
        create_scraped_document(phone_number, post_title.encode('utf-8'), document['ad_url'], document['ad_html'].encode('utf-8'))

        # Update existing blacklist document so we don't double process...
        set_field_blacklisted_document(document, 'phone_number', phone_number)
        set_field_blacklisted_document(document, 'reason', 'scraped')


if __name__ == "__main__":
    queue = []

    number_thread  = threading.Thread(target=get_no_number_documents)
    number_thread.start()
    #
    # while(len(queue) == 0):
    #     continue
    #
    # threads = []
    # THREAD_COUNT = 1
    # start = time.time()
    # for i in range(THREAD_COUNT):
    #     t = threading.Thread(target=process_numbers_in_blacklist_documents)
    #     threads.append(t)
    # for t in threads:
    #     t.start()
    #     t.join()
    # #process_numbers_in_blacklist_documents()
    # end = time.time()
    # print("%s" % (end - start))
    # print("Done?")

# csv_file = open("parsed_bodies_finalv2.csv", 'w')
# documents = get_no_number_documents()
# # documentid = collection.find('_id')
# # print(documentid)
# mynumbers = 0
# totalnumbers = 0
#
# csv_file.close()
# if mylist >= 1000
#     exit()
# csv_file.close()

# if parsed_html
# f = open('list.csv', 'w')
# f.write('Text\n')
# for parsed_htmls in parsed_html
#     f.write(parsed_html)
#     f.close()
