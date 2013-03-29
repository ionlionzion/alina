from alina.common import IllegalArgumentException, IllegalStateException
import json

class PublisherException(Exception):
    pass

class Publisher(object):
    
    def __init__(self):
        self.is_open = False
    
    def publish(self, item):
        """Publish an item."""
        if not self.is_open:
            raise IllegalStateException("Resource not open.")
    
    def close(self):
        """Close any resources that this Publisher used."""
        pass
    
    def prepare(self):
        """Open any resources needed by this Publisher. 
        Must be called before publish. If publish is called 
        and prepare is not called, an IllegalStateException should
        be thrown."""
        self.is_open = True
        
class JsonPublisher(Publisher):
    
    def __init__(self, path, meta_path):
        super(Publisher, self).__init__()
        if path is None:
            raise IllegalArgumentException("You must supply a path to publish objects.")
        self.path = path
        self._file = None
        self._first = True

    def prepare(self):
        Publisher.prepare(self)
        try:
            self._file = open(self.path, 'w')
        except IOError:
            raise PublisherException("error opening file " + self.path)
        
    def close(self):
        if not self._first: #something was written
            self._file.write("]")
        if self._file is not None:
            self._file.close()
            
    def publish(self, item):
        Publisher.publish(self, item)
        if self._first:
            self._first = False
            self._file.write("[")
        else:
            self._file.write(",")
        json.dump(item, self._file, encoding = 'utf-8')
        
        
class CsvPublisher(Publisher):
    
    def __init__(self, path):
        super(Publisher, self).__init__()
        if path is None:
            raise IllegalArgumentException("You must supply a path to publish objects.")
        self.path = path
        self._file = None
    
    def prepare(self):
        Publisher.prepare(self)
        try:
            self._file = open(self.path, 'w')
        except IOError:
            raise PublisherException("error opening file " + self.path)
        
    def close(self):
        if self._file is not None:
            self._file.close()
            
    def publish(self, item):
        Publisher.publish(self, item)
        self._file.write(item)
        self._file.write("\n")


class WordCounter:
    
    def __init__(self):
        self.words = {}
    
    def prepare(self):
        pass
    
    def close(self):
        pass
    
    #elem should be a word array
    def publish(self, elems):
        for elem in elems:
            if self.words.has_key(elem):
                self.words[elem] = self.words[elem] + 1
            else:
                self.words[elem] = 1
                
    def dump(self, _file):
        try:
            _fdes = open(_file, 'w')
            sort = sorted(self.words.items(), key = lambda (t): -1 * t[1])
            for pair in sort:
                _fdes.write(pair[0] + "=" + str(pair[1]))
                _fdes.write('\n')
        finally:
            if _fdes is not None:
                _fdes.close()