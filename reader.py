from alina.common import IllegalArgumentException
import facebook
import json
import datetime

class FileReaderIterator(object):
    
    def __init__(self, path, read_fun):
        if path is None:
            raise IllegalArgumentException("you must supply a path to read from.")
        if read_fun is None:
            raise IllegalArgumentException("you must supply a reading function.")
        self._stream = open(path, 'r')
        self.read_function = read_fun
        self._next = self._read()
    
    def _read(self):
        return self.read_function(self._stream)
    
    def __iter__(self):
        return self
    
    def next(self):
        if self._next is None:
            try:
                if self._stream is not None:
                    self._stream.close()
            except:
                #catch any error
                pass
            
            raise StopIteration
        ret = self._next
        self._next = self._read()
        return ret


def json_read(json_stream):
    val = json_stream.read()
    if val == '' or val is None:
        return None
    return json.loads(val, encoding = 'utf-8')
    
def json_streaming_read(json_stream):
    #TODO finish me
    pass
        
def csv_read(csv_stream):
    #TODO
    pass
    
class FacebookPostsReaderIterator:
    """Reads all Facebook posts of a person."""
    
    def __init__(self, person, token, last_post_date, limit = 25):
        self.person = person
        self.token = token
        self.last_post_date = last_post_date
        self.limit = limit
        self.graph = facebook.GraphAPI(token)
        self.id = self._get_id()
        self.until = None
        self._main_query = True
        self._current_date = datetime.datetime.now()
        self._day_shift = 90
        #advance in the posts
        self._next_page()
    
    def _get_id(self):
        profile = self.graph.get_object(self.person)
        return profile['id']
    
    def _next_page(self):
        post_args = {
            'access_token': self.token,
            'limit': self.limit,
        }
        
        path = self.id + "/posts" # try /feed too
        
        if self._main_query:
            post_args['until'] = self._date_to_str(self._current_date)
            #go back _day_shift number of days
            self._current_date = self._minus_n_days(self._current_date, self._day_shift)
            post_args['since'] = self._date_to_str(self._current_date)
            #go back one more day!
            self._current_date = self._minus_n_days(self._current_date, 1)
            self._main_query = False
            #make request
            self._request_and_advance(path, post_args)
        else:
            post_args['until'] = self.until
            #make request
            self._request_and_advance(path, post_args)
            
            #end of this page
            if self._list is None or len(self._list) == 0:
                #move to next X days
                self._main_query = True
                #advance
                self._next_page()
    
    def _request_and_advance(self, path, post_args):
        #get the data
        data = self.graph.request(path, post_args)
        #the actual results
        self._list = data['data']
        #until will change
        if data.has_key('paging'):
            next_url = data['paging']['next']
            self.until = self._get_until(next_url)
    
    def _get_until(self, url):
        #"https://graph.facebook.com/<ID>/posts?limit=<LIMIT>&until=<UNTIL>"
        from urlparse import urlparse, parse_qs
        _actual_url = urlparse(url)
        query = parse_qs(_actual_url.query)
        if query.has_key('until'):
            #hacky, but there's only one until
            return str(query['until'][0])
        return None
    
    def __iter__(self):
        return self
    
    def next(self):
        if self._list is None or len(self._list) == 0:
            raise StopIteration
        value = self._list
        self._next_page()
        return value
    
    def _date_to_str(self, now):
        return now.strftime("%Y-%m-%d")
            
    def _minus_n_days(self, now, days):
        return now - datetime.timedelta(days = days)