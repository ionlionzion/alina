# prep work
import sys
import os
basepath = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(basepath, "..")))

from alina.publisher import CsvPublisher, JsonPublisher, WordCounter
from alina.reader import FileReaderIterator, json_read
from alina.util import collect, convert_json_to_clean_csv, between, \
    convert_json_to_word_list, collect_facebook_posts, \
    convert_fbk_json_to_simple_json, collect_posts_and_count_words

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

(options, args) = parser.parse_args()

if options.token is None:
	print "No specified token"
	sys.exit()

if options.persons is None:
	print "No persons"
	sys.exit()
	
persons = options.persons.split(',')
	
if __name__ == '__main__':
	# https://developers.facebook.com/tools/explorer/ (get access token)
	#token = "AAACEdEose0cBAPFbxZC2BccVXjzOD6fyHTFisJ2cbgPrwkBjhMob9pqm8decWWiaCZCy6aVMgWAF11Lbsh9zZCPIANED63ClaDuEtcaBQZDZD"
	
	collect_posts_and_count_words(persons, options.token, since = options.since, until = options.to)
