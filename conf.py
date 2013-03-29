# coding=utf-8
STOP_WORDS = []
    
def get_stop_words():
    if len(STOP_WORDS) != 0:
        return STOP_WORDS
    stop_words_file = open('conf/stop_words.txt', 'r')
    for line in stop_words_file.readlines():
        word = line.replace('\n', '')
        STOP_WORDS.append(word)
    stop_words_file.close()
    return STOP_WORDS

REPLACEMENTS = {}

def get_replacements():
    if len(REPLACEMENTS) != 0:
        return REPLACEMENTS
    _file = open('conf/replacements.txt', 'r')
    for line in _file.readlines():
        word = line.replace('\n', '')
        kv = word.split('=')
        REPLACEMENTS[kv[0]] = kv[1]
    _file.close()
    return REPLACEMENTS

def replace(word):
    if type(word) == unicode:
        ret = unicode.encode(word, 'utf-8')
    else:
        ret = word
    
    for key, value in get_replacements().items():
        ret = ret.replace(key, value)

    return ret
