from alina.conf import get_stop_words, replace
from alina.porter_stemmer import PorterStemmer
from alina.publisher import CsvPublisher, JsonPublisher, WordCounter
from alina.reader import FacebookPostsReaderIterator, FileReaderIterator, json_read
import inflect
import re
import os

STEMMER = PorterStemmer()
INFLECT = inflect.engine()

def _to_str(data):
    ret = {}
    for key, value in data.items():
        ret[_encode(key)] = _encode(value)
    return ret
        
def _encode(value):
    return unicode.encode(value, 'utf-8')    

def collect_facebook_posts(name, token, folder, last_post_date=None, page_size=25):    
    fbk_reader = FacebookPostsReaderIterator(name, token,
                                         last_post_date, limit=page_size)

    json_pub = JsonPublisher(folder + "/" + name + ".txt", folder + "/meta_" + name + ".txt")
    collect(fbk_reader, json_pub)

def same(elem):
    return elem

def no_filter(elem):
    return True

def collect(reader, publisher, convert_function=same, filter_function=no_filter):
    try:
        publisher.prepare()
        for page in reader:
            for elem in page:
                if filter_function(elem):
                    publisher.publish(convert_function(elem))
    finally:        
        publisher.close()

def convert_fbk_json_to_simple_json(json_obj):
    message = ''
    if json_obj.has_key('message'):
        message = json_obj['message']
    
    return {'name' : json_obj['from']['name'],
            'message' : message,
            'created_time' : json_obj['created_time']}

def convert_json_to_csv(json_obj):
    message = ''
    if json_obj.has_key('message'):
        message = json_obj['message']
        
    return ",".join([json_obj['from']['name'], message, json_obj['created_time']])

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

def clean_string(word):
    return clean(word, lambda(w): INFLECT.singular_noun(w) or w)

def clean_as_list(word, fun=same):
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
    
    # 6 apply the fun
    return [fun(w) for w in words if w not in get_stop_words()]

def clean(word, fun=same):
    return " ".join(clean_as_list(word, fun))

def lower(word):
    return word.lower()

def clean_stem(word):
    return clean(word, lambda(w): STEMMER.stem2(word))

def convert_json_to_clean_csv(json_obj):
    message = ''
    if json_obj.has_key('message'):
        message = json_obj['message']
        
    return ",".join([json_obj['from']['name'], clean_string(message), json_obj['created_time']])

def convert_json_to_word_list(json_obj):
    message = ''
    if json_obj.has_key('message'):
        message = json_obj['message']
    return clean_as_list(message)

def collect_posts(persons, token, dest):
    create_folder(dest)
    for person in persons:
        collect_facebook_posts(person, token, dest)

def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

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

def get_files_in_folder(target):
    return [ f for f in os.listdir(target) if os.path.isfile(os.path.join(target, f)) ]
            
def count_words(target, dest, since=None, until=None):
    # create the destination folder
    create_folder(dest)
    # get files
    onlyfiles = get_files_in_folder(target)
    
    filter_fun = create_filter(since, until)
    
    for _file in onlyfiles:
        w = WordCounter()
        collect(FileReaderIterator(os.path.join(target, _file), json_read),
				w,
				convert_function=convert_json_to_word_list,
				filter_function=filter_fun)

        w.dump(os.path.join(dest, _file))
        
def to_clean_text(msg):
    return " ".join(convert_json_to_word_list(msg))

def to_csv(msg):
    name = msg['from']['name']
    message = to_clean_text(msg)
    created_time = msg['created_time']
    return ",".join([name, message, created_time[:10]])
 
def messages_to_csv(target, dest, since=None, until=None):
    # create the destination folder
    create_folder(dest)
    # get files
    onlyfiles = get_files_in_folder(target)
    
    filter_fun = create_filter(since, until)
    
    for _file in onlyfiles:
        _dest_file = re.sub(r'.txt', '.csv', _file)
        pub = CsvPublisher(os.path.join(dest, _dest_file))
        collect(FileReaderIterator(os.path.join(target, _file), json_read),
				pub,
				convert_function=to_csv,
				filter_function=filter_fun)
