import re
from pigeon.logger import Logger

class APIResource(object):

    SET = 0x01
    GET = 0x02

    def __init__(self, name, attr, callback):
        self.name = name
        self.cb = callback
        self.attr = attr
        self.parent = None
        self.children = []


class APIHandler(object):


    def __init__(self, filepath):
        self.callbacks = {}
        self.log = Logger()
        self._root = None
        self.uri = ""
        self.method = ""
        self.data = {}
        self.parsed_params = {}
        self._create(filepath)

    def _print_api(self, root, level=0):
        print("%s %s" % (''.ljust(level+1,'-'), root.name))
        for child in root.children:
            self._print_api(child, level+1)

    def _create(self, filepath):
        file = open(filepath, "r")

        max_level = 0
        for line in file:
            level = line.count('-')
            if level > max_level:
                max_level = level
        file.close()
        parents = [None]*max_level

        file = open(filepath, "r")
        for line in file:
            line = line[:-1]
            if line[0] == '#':
                continue

            line = line.replace(' ', '')
            line = line.replace('\t', '')
            fields = line.split(',')

            if len(fields) != 3:
                print "ERROR parsing %s (%s)" % (filepath, line)
                continue

            level = fields[0].count('-')

            rsrc_name = fields[0].replace('-','')
            rsrc_attr = 0
            if 's' in fields[1]: rsrc_attr |= APIResource.SET
            if 'g' in fields[1]: rsrc_attr |= APIResource.GET
            cb_name = fields[2]

            cb = None
            if "*" in cb_name:
                print "Callback of %s is intentionally not implemented" % rsrc_name
            else:
                try:
                    cb = getattr(self, "api_" + cb_name)
                except:
                    raise Exception("Callback %s not implemented" % cb_name)

            api_rsrc = APIResource(rsrc_name, rsrc_attr, cb)

            if level == 0:
                if self._root is None:
                    self._root = api_rsrc
                else:
                    raise Exception("Multiple roots")
            else:
                parent = parents[level-1]
                if parent is None:
                    raise Exception("%s has no parent" % rsrc_name)
                api_rsrc.parent = parent
                parent.children.append(api_rsrc)
            parents[level] = api_rsrc

        #self._print_api(self._root)

    def run(self, method, uri, data={}):
        uri = uri.replace('\n', '')
        uri = uri.replace('\r', '')
        self.uri = uri

        if method != "get" and method != "set":
            raise Exception("Method must be \"get\" or \"set\"")
        self.method = method

        resources = uri.split('/')
        resources = filter(None, resources)

        self.parsed_params.clear()
        cur_rsrc = self._root
        first_rsrc_name = resources[0]

        if first_rsrc_name != cur_rsrc.name:
            print "%s is not the name of the first resource (%s)" % (first_rsrc_name, cur_rsrc.name)
            return

        for rsrc_name in resources[1:]:

            next_rsrc = None
            for child in cur_rsrc.children:
                if child.name == rsrc_name:
                    next_rsrc = child
                    break

            if next_rsrc is None:
                for child in cur_rsrc.children:
                    if '{' in child.name:
                        param_name = child.name
                        param_name = param_name.replace('{', '')
                        param_name = param_name.replace('}', '')
                        param_value = rsrc_name
                        if param_value.isdigit():
                            param_value = int(param_value)
                        self.parsed_params.update({param_name: param_value})
                        next_rsrc = child
                        break

            if next_rsrc is not None:
                cur_rsrc = next_rsrc
            else:
                print "Parsing error: resource %s not found" % rsrc_name
                return

        if (method == "get" and (cur_rsrc.attr & APIResource.GET) == 0) or \
           (method == "set" and (cur_rsrc.attr & APIResource.SET) == 0):
            print "Unsupported method name: %s %s" % (method, rsrc_name)
            return

        if cur_rsrc.cb is not None:
            print "Call %s" % cur_rsrc.cb.__name__
            if self._validate_parsed_params() is True:
                cur_rsrc.cb()
        else:
            print "Callback of %s is not implemented" % cur_rsrc.name
            return

    def _validate_parsed_params(self):
        return True


    def set(self, uri, data={}):
        self.run(uri, "set", data)

    def get(self, uri, data={}):
        self.run(uri, "get", data)
