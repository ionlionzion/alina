from alina.conf import get_stop_words, replace
from alina.porter_stemmer import PorterStemmer
from alina.publisher import CsvPublisher, JsonPublisher, WordCounter
from alina.reader import FacebookPathReaderIterator, FileReaderIterator, json_read
import inflect
import re
import os

STEMMER = PorterStemmer()
INFLECT = inflect.engine()

########################## PROCESSORS ##########################

def clean_stem(word):
    return " ".join( [ STEMMER.stem2(w) for w in clean_as_list(word)])

def clean_string(word):
    return " ".join( [ INFLECT.singular_noun(w) or w for w in clean_as_list(word)])

def raw(word):
    return replace(word)

def lower(word):
    return word.lower()

def clean_as_list(word):
    # 0 clear any URL
    word = re.sub(r"\.", '', word)
    word = re.sub(r"http://\w+(/\w*)?", '', word)
    
    # 1 make the replacements
    text = replace(word)
    text = re.sub(r"\n", '', text)

    # 2 get only the words
    
    # 3 lower
    words = map(lower, re.findall(r"\w+\'?\w+", text))
    
    # 4 remove stop words
    return [w for w in words if w not in get_stop_words()]

PROCESSORS = {"raw" : raw, "stem" : clean_stem, "clean" : clean_string}

########################## PROCESSORS ##########################

########################## FILE UTIL ##########################

def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_files_in_folder(target):
    return [ f for f in os.listdir(target) if os.path.isfile(os.path.join(target, f)) ]

########################## FILE UTIL ##########################

########################## DATE FILTER ##########################

def between(date1, date2):
    def _between(elem):
        if elem.has_key('created_time'):
            c_time = elem['created_time'][:10]
            return c_time >= date1 and c_time <= date2
        return False
    return _between

def before(date1):
    def _before(elem):
        if elem.has_key('created_time'):
            c_time = elem['created_time'][:10]
            return c_time <= date1
        return False
    return _before    

def after(date1):
    def _after(elem):
        if elem.has_key('created_time'):
            c_time = elem['created_time'][:10]
            return c_time >= date1
        return False
    return _after

def create_filter(since=None, until=None):
    if since is None:
        if until is None:
            return no_filter
        else:
            return before(until)    
    else:
        if until is None:
            return after(since)
        else:
            return between(since, until)
        
def no_filter(elem):
    return True

########################## DATE FILTER ##########################
    
########################## ACTIONS ##########################

def raw_text(target, dest, processor, since=None, until=None):
    simple_process(target, dest, lambda x: x, get_message_from_json, processor, since, until)

def messages_to_csv(target, dest, processor, since=None, until=None):
    simple_process(target, dest, lambda x: re.sub(r'.txt', '.csv', x), to_csv, processor, since, until)

def count_words(target, dest, processor, since=None, until=None):
    # create the destination folder
    create_folder(dest)
    # get files
    onlyfiles = get_files_in_folder(target)
    
    filter_fun = create_filter(since, until)
    
    for _file in onlyfiles:
        w = WordCounter()
        collect(FileReaderIterator(os.path.join(target, _file), json_read),
                w,
                processor,
                convert_function=get_message_from_json,
                filter_function=filter_fun)

        w.dump(os.path.join(dest, _file))

def collect_posts(persons, dest):
    create_folder(dest)
    for person in persons:
        collect_facebook_path(person, dest, 'posts')

def collect_feed(persons, dest):
    create_folder(dest)
    for person in persons:
        collect_facebook_path(person, dest, 'feed')

########################## ACTIONS ##########################

def get_processor(processor):
    return PROCESSORS[processor]

def same(elem, processor):
    return elem

def get_message_from_json(json_obj, processor):
    message = ''
    if json_obj.has_key('message'):
        message = json_obj['message']
    return get_processor(processor)(message)

def to_csv(msg, processor):
    name = msg['from']['name']
    message = ''
    if msg.has_key('message'):
        message = get_processor(processor)(msg['message'])
    created_time = msg['created_time']
    return ",".join([name, message, created_time[:10]])

def collect_facebook_path(name, folder, uri_path, page_size=25):    
    fbk_reader = FacebookPathReaderIterator(name, uri_path, limit=page_size)
    json_pub = JsonPublisher(folder + "/" + name + "_" + uri_path + ".txt")
    collect(fbk_reader, json_pub, "raw", same, no_filter)

def collect(reader, publisher, processor, convert_function, filter_function):
    try:
        publisher.prepare()
        for page in reader:
            for elem in page:
                if filter_function(elem):
                    publisher.publish(convert_function(elem, processor))
    finally:        
        publisher.close()

def simple_process(target, dest, dest_file_fun, convert_fun, processor, since=None, until=None):
    # create the destination folder
    create_folder(dest)
    # get files
    onlyfiles = get_files_in_folder(target)
    
    filter_fun = create_filter(since, until)
    
    for _file in onlyfiles:
        _dest_file = dest_file_fun(_file)
        pub = CsvPublisher(os.path.join(dest, _dest_file))
        collect(FileReaderIterator(os.path.join(target, _file), json_read),
                pub,
                processor,
                convert_function=convert_fun,
                filter_function=filter_fun)
