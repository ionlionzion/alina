# prep work
import sys
import os
basepath = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(basepath, "..")))

from alina.util import collect_posts, collect_feed, count_words, \
    messages_to_csv, raw_text

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
parser.add_option("-r", "--processor",
                  dest="processor", default="raw",
                  help="the way the text is processed: raw(no processing), clean(remove stop words, to lower, remove plural), stem(clean and apply a Porter Stemmer")
parser.add_option("-x", "--action",
                  dest="action", default=None,
                  help="valid options are get_posts, get_feed, word_count, raw_text and to_csv")

(options, args) = parser.parse_args()

def raw_text_action(options):
    if options.target is None:
        print "you must supply a target folder"
        sys.exit()
    if options.destination is None:
        print "you must supply an output folder"
        sys.exit()
    raw_text(options.target, options.destination, options.processor, since = options.since, until = options.to)
 
def to_csv_action(options):
    if options.target is None:
        print "you must supply a target folder"
        sys.exit()
    if options.destination is None:
        print "you must supply an output folder"
        sys.exit()
    messages_to_csv(options.target, options.destination, options.processor, since = options.since, until = options.to)

def get_posts_action(options):
    if options.token is None:
        print "No specified token"
        sys.exit() 
    if options.persons is None:
        print "No persons"
        sys.exit()
    persons = options.persons.split(',')
    collect_posts(persons, options.token, os.path.join(basepath, "posts"))
    
def get_feed_action(options):
    if options.token is None:
        print "No specified token"
        sys.exit() 
    if options.persons is None:
        print "No persons"
        sys.exit()
    persons = options.persons.split(',')
    collect_feed(persons, options.token, os.path.join(basepath, "feed"))

def word_count_action(option):
    if options.target is None:
        print "you must supply a target folder"
        sys.exit()
    if options.destination is None:
        print "you must supply an output folder"
        sys.exit()
    count_words(options.target, options.destination, options.processor, since = options.since, until = options.to)
    
actions = {"get_posts" : get_posts_action, 
           "word_count" : word_count_action, 
           "to_csv" : to_csv_action, 
           "raw_text" : raw_text_action,
           "get_feed" : get_feed_action}   

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