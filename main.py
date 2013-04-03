# prep work
import sys
import os
basepath = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(basepath, "..")))

from alina.publisher import CsvPublisher, JsonPublisher, WordCounter
from alina.reader import FileReaderIterator, json_read
from alina.util import collect, convert_json_to_clean_csv, between, \
    convert_json_to_word_list, collect_facebook_posts, \
    convert_fbk_json_to_simple_json, collect_posts, count_words

from optparse import OptionParser
parser = OptionParser()
parser.add_option("-f", "--from", dest="since", default = None,
                  help="from date", metavar="FROM")
parser.add_option("-t", "--to",
                  dest="to", default=None,
                  help="to date")
parser.add_option("-k", "--token",
                  dest="token", default=None,
                  help="facebook dev token")
parser.add_option("-p", "--persons",
                  dest="persons", default=None,
                  help="persons to get posts from")
parser.add_option("-g", "--target",
                  dest="target", default=None,
                  help="target folder for processing")
parser.add_option("-o", "--output",
                  dest="destination", default=None,
                  help="destination folder for processing results")
parser.add_option("-x", "--action",
                  dest="action", default=None,
                  help="valid options are get_posts and word_count")

(options, args) = parser.parse_args()
    
def get_posts(options):
    if options.token is None:
        print "No specified token"
        sys.exit() 
    if options.persons is None:
        print "No persons"
        sys.exit()
    persons = options.persons.split(',')
    collect_posts(persons, options.token, os.path.join(basepath, "posts"))

def word_count(option):
    if options.target is None:
        print "you must supply a target folder"
        sys.exit()
    if options.destination is None:
        print "you must supply an output folder"
        sys.exit()
    count_words(options.target, options.destination, since = options.since, until = options.to)
    
actions = {"get_posts" : get_posts, "word_count" : word_count}   

def delegate(options):
    action = options.action
    if not actions.has_key(action):
        print "No such action " + action
        sys.exit()
    actions[action](options)

if __name__ == '__main__':
	# https://developers.facebook.com/tools/explorer/ (get access token)
	# token = "AAACEdEose0cBAPFbxZC2BccVXjzOD6fyHTFisJ2cbgPrwkBjhMob9pqm8decWWiaCZCy6aVMgWAF11Lbsh9zZCPIANED63ClaDuEtcaBQZDZD"	

    delegate(options)