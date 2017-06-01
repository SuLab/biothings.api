import json, logging, re
from biothings.utils.common import dotdict, is_str, is_seq, find_doc
from biothings.utils.es import get_es
from biothings.utils.userquery import get_userquery, get_userfilter
from elasticsearch import NotFoundError, RequestError, TransportError
from biothings.settings import BiothingSettings
#from biothings.utils.dotfield import compose_dot_fields_by_fields as compose_dot_fields
from collections import OrderedDict

biothing_settings = BiothingSettings()

# ES related Helper func
def parse_sort_option(options):
    sort = options.get('sort', None)
    if sort:
        _sort_array = []
        for field in sort.split(','):
            field = field.strip()
            if field == 'name' or field[1:] == 'name':
                 # sorting on "name" field is ignored, as it is a multi-text field.
                 continue
            if field.startswith('-'):
                _f = {"%s" % field[1:]: "desc"}
            else:
                _f = {"%s" % field: "asc"}
            _sort_array.append(_f)
        options["sort"] = _sort_array
    return options

def parse_facets_option(kwargs):
    aggs = kwargs.pop('aggs', None)
    if aggs:
        _aggs = {}
        for field in aggs.split(','):
            _aggs[field] = {"terms": {"field": field}}
        return _aggs


class QueryError(Exception):
    pass


class ScrollSetupError(Exception):
    pass


class ESQuery(object):
    def __init__(self):
        self._es = get_es(biothing_settings.es_host)
        self._index = biothing_settings.es_index
        self._doc_type = biothing_settings.es_doc_type
        self._allowed_options = biothing_settings.allowed_options
        self._scroll_time = biothing_settings.scroll_time
        self._total_scroll_size = biothing_settings.scroll_size   # Total number of hits to return per scroll batch
        self._default_fields = []
        try:
            self._context = json.load(open(biothing_settings.jsonld_context_path, 'r'))
        except FileNotFoundError:
            self._context = {}
        if self._total_scroll_size % self.get_number_of_shards() == 0:
            # Total hits per shard per scroll batch
            self._scroll_size = int(self._total_scroll_size / self.get_number_of_shards())
        else:
            raise ScrollSetupError("_total_scroll_size of {} can't be ".format(self._total_scroll_size) +
                                     "divided evenly among {} shards.".format(self.get_number_of_shards()))

    def _traverse_biothingdoc(self, doc, context_key, dotfield_ret, options=None):
        # Traverses through all levels of biothing doc to add jsonld context and sort the dictionaries
        if isinstance(doc, list):
            return [self._traverse_biothingdoc(d, context_key, dotfield_ret, options) for d in doc]
        elif isinstance(doc, dict):
            this_list = []
            for key in sorted(doc):
                new_key = key if not context_key else context_key + '.' + key
                this_list.append( (key, self._traverse_biothingdoc(doc[key], new_key, dotfield_ret, options)) )
            return OrderedDict(this_list)
        else:
            if options.dotfield or options.jsonld:
                # jsonld option doesn't play nice with dotfields, if jsonld=true is set it overrides dotfield
                dotfield_ret.setdefault(context_key, []).append(doc)
            return doc

    def _get_biothingdoc(self, hit, options=None):
        doc = hit.get('_source', hit.get('fields', {}))
        doc.setdefault('_id', hit['_id'])
        for attr in ['_score', '_version']:
            if attr in hit:
                doc.setdefault(attr, hit[attr])

        if hit.get('found', None) is False:
            # if found is false, pass that to the doc
            doc['found'] = hit['found']
        #TODO: normalize, either _source or fields...
        fields = options.kwargs.fields or options.kwargs._source
        # add other keys to object, if necessary
        doc = self._modify_biothingdoc(doc=doc, options=options)
        # Sort keys, and add jsonld
        dotfield_ret = {}
        doc = self._traverse_biothingdoc(doc=doc, context_key='', 
            dotfield_ret=dotfield_ret, options=options)
        if options.jsonld:
            return OrderedDict([(k, v[0]) if len(v) == 1 else (k,v) for (k,v) in [('@context', self._context['@context']), ('@id', 'http://mygene.info/v3/gene/{}'.format(doc['_id']))] + 
                    sorted(dotfield_ret.items(), key=lambda i: i[0])])
        elif options.dotfield:
            return OrderedDict([(k, v[0]) if len(v) == 1 else (k,v) for (k,v) in sorted(dotfield_ret.items(), key=lambda i: i[0])])
        return doc

    def _modify_biothingdoc(self, doc, options=None):
        # function for overriding in subclass
        return doc

    def _cleaned_res(self, res, empty=[], error={'error': True}, single_hit=False, options=None):
        '''res is the dictionary returned from a query.
           do some reformating of raw ES results before returning.

           This method is used for self.mget_biothings and self.get_biothing method.
        '''
        if 'error' in res:
            return error

        hits = res['hits']
        total = hits['total']
        if total == 0:
            return empty
        elif total == 1 and single_hit:
            return self._get_biothingdoc(hit=hits['hits'][0], options=options)
        else:
            return [self._get_biothingdoc(hit=hit, options=options) for hit in hits['hits']]

    def _cleaned_res2(self, res, options=None):
        '''res is the dictionary returned from a query.
           do some reformating of raw ES results before returning.

           This method is used for self.query method.
        '''
        if 'aggregations' in res:
            # need to normalize back to what "facets" used to return
            # (mostly key renaming + total computation)
            res["facets"] = res.pop("aggregations")
            for facet in res["facets"]:
                # restuls always coming from terms aggregations
                res["facets"][facet]["_type"] = "terms"
                res["facets"][facet]["terms"] = res["facets"][facet].pop("buckets")
                res["facets"][facet]["other"] = res["facets"][facet].pop("sum_other_doc_count")
                res["facets"][facet]["missing"] = res["facets"][facet].pop("doc_count_error_upper_bound")
                count = 0
                for term in res["facets"][facet]["terms"]:
                    # modif in-place
                    term["count"] = term.pop("doc_count")
                    term["term"] = term.pop("key")
                    count += term["count"]
                res["facets"][facet]["total"] = count

        _res = res['hits']
        for attr in ['took', 'facets', '_scroll_id']:
            if attr in res:
                _res[attr] = res[attr]
        _res['hits'] = [self._get_biothingdoc(hit=hit, options=options) for hit in _res['hits']]
        _resf = OrderedDict([(k, v) for (k, v) in sorted(_res.items(), key=lambda i: i[0]) 
                                if k != 'hits'])
        _resf['hits'] = _res['hits'] 
        return _resf

    def _cleaned_scopes(self, scopes):
        '''return a cleaned scopes parameter.
            should be either a string or a list of scope fields.
        '''
        if scopes:
            if is_str(scopes):
                scopes = [x.strip() for x in scopes.split(',')]
            if is_seq(scopes):
                scopes = [x for x in scopes if x]
                if len(scopes) == 1:
                    scopes = scopes[0]
            else:
                scopes = None
        else:
            scopes = None
        return scopes

    def _cleaned_fields(self, fields):
        '''return a cleaned fields parameter.
            should be either None (return all fields) or a list fields.
        '''
        if fields:
            if is_str(fields):
                if fields.lower() == 'all':
                    fields = None     # all fields will be returned.
                else:
                    fields = [x.strip() for x in fields.split(',')]
        else:
            fields = self._default_fields
        return fields

    def _get_options(self, options, kwargs):
        ''' Function to override to add more options to the get_cleaned_query_options function below .'''
        return options

    def _get_cleaned_metadata_options(self, kwargs):
        ''' Process options for /metadata query. '''
        options = dotdict()
        # Delete all keys, can override this to add arguments to metadata endpoint
        for key in set(kwargs.keys()):
            del(kwargs[key])
        return options

    def _get_cleaned_common_options(self, kwargs):
        '''process options whatever the type of query (/query or annotation)'''
        options = dotdict()
        options.raw = kwargs.pop('raw', False)
        options.rawquery = kwargs.pop('rawquery', False)
        options.fetch_all = kwargs.pop('fetch_all', False)
        options.host = kwargs.pop('host', biothing_settings.ga_tracker_url)
        options.jsonld = kwargs.pop('jsonld', False)
        options.dotfield = kwargs.pop('dotfield', False)
        options.userquery = kwargs.pop('userquery', False)
        options.userquery_kwargs = dict([(k[3:], v) for (k, v) in kwargs.items() if re.match(r'^uq_.*$', k)])
        
        # override to add more options
        options = self._get_options(options, kwargs)
        scopes = kwargs.pop('scopes', None)
        if scopes:
            options.scopes = self._cleaned_scopes(scopes)
        kwargs = parse_sort_option(kwargs)
        for key in set(kwargs) - set(self._allowed_options):
            logging.debug("removing param '%s' from query" % key)
            del kwargs[key]
        return options

    def _get_cleaned_query_options(self, kwargs):
        """common helper for processing fields, kwargs and other options passed to ESQueryBuilder."""
        options = self._get_cleaned_common_options(kwargs)
        fields = kwargs.pop('fields', None)
        # this will force returning default fields if none were passed
        fields = self._cleaned_fields(fields)
        if fields:
            kwargs["_source"] = fields
        options.kwargs = kwargs
        return options

    def _get_cleaned_annotation_options(self, kwargs):
        """common helper for processing fields, kwargs and other options passed to ESQueryBuilder."""
        options = self._get_cleaned_common_options(kwargs)
        # return all fields if none were passed
        fields = kwargs.pop('fields', None)
        if fields:
            fields = self._cleaned_fields(fields)
            if fields:
                kwargs["_source"] = fields
        options.kwargs = kwargs
        return options

    def get_number_of_shards(self):
        r = self._es.indices.get_settings(self._index)
        n_shards = r[list(r.keys())[0]]['settings']['index']['number_of_shards']
        n_shards = int(n_shards)
        return n_shards

    def exists(self, bid):
        """return True/False if a biothing id exists or not."""
        try:
            doc = self.get_biothing(bid, fields=None)
            return doc['found']
        except NotFoundError:
            return False

    def _get(self, **kwargs):
        """ Subclass for /annotation GET es query override. """
        options = kwargs.pop('options', {})
        return self._es.get(**kwargs)

    def get_biothing(self, bid, **kwargs):
        '''unknown vid return None'''
        options = self._get_cleaned_annotation_options(kwargs)
        kwargs = {"_source": options.kwargs["_source"]} if "_source" in options.kwargs else {}
        try:
            res = self._get(index=self._index, id=bid, doc_type=self._doc_type, options=options, **kwargs)
        except NotFoundError:
            return
        
        if options.raw:
            return res

        res = self._get_biothingdoc(res, options=options)
        return res

    def _msearch(self,**kwargs):
        kwargs.pop('options', {})
        return self._es.msearch(**kwargs)['responses']

    def mquery_biothings(self,bid_list, **kwargs):
        options = self._get_cleaned_query_options(kwargs)
        return self.mcommon_biothings(bid_list, options, **kwargs)

    def mget_biothings(self, bid_list, **kwargs):
        '''for /query post request'''
        options = self._get_cleaned_annotation_options(kwargs)
        return self.mcommon_biothings(bid_list, options, **kwargs)

    def mcommon_biothings(self,bid_list, options, **kwargs):
        qbdr = self._get_query_builder(options=options, **options.kwargs)
        try:
            _q = qbdr.build_multiple_id_query(bid_list, scopes=options.scopes)
        except QueryError as err:
            return {'success': False,
                    'error': err.message}
        if options.rawquery:
            return _q

        res = self._msearch(body=_q, index=self._index, doc_type=self._doc_type, options=options)
        
        if options.raw:
            return res

        assert len(res) == len(bid_list)
        _res = []

        for i in range(len(res)):
            hits = res[i]
            qterm = bid_list[i]
            hits = self._cleaned_res(hits, empty=[], single_hit=False, options=options)
            if len(hits) == 0:
                _res.append({u'query': qterm,
                             u'notfound': True})
            elif 'error' in hits:
                _res.append({u'query': qterm,
                             u'error': True})
            else:
                for hit in hits:
                    hit[u'query'] = qterm
                    _res.append(hit)
        return _res

    def _get_query_builder(self,**kwargs):
        '''Subclass to get a custom query builder'''
        return ESQueryBuilder(**kwargs) 

    def _build_query(self, q, **kwargs):
        # can override this function if more query types are to be added
        esqb = self._get_query_builder(**kwargs)
        return esqb.default_query(q)

    def _search(self, q, **kwargs):
        '''Subclass to get a custom search query'''
        options = kwargs.pop('options', {})
        # since all args are ES compatible, we can send them all
        return self._es.search(index=self._index, doc_type=self._doc_type, body=q, **kwargs)

    def query(self, q, **kwargs):
        # clean
        if type(q) == list:
            return {"success" : False, "error": "Only one 'q' parameter allowed"}
        q = re.sub(u'[\t\n\x0b\x0c\r\x00]+', ' ', q)
        q = q.strip()

        aggs = parse_facets_option(kwargs)
        options = self._get_cleaned_query_options(kwargs)
        # for scroll type
        scroll_options = {}
        if options.fetch_all:
            scroll_options.update({'size': self._total_scroll_size, 'scroll': self._scroll_time})
        options["kwargs"].update(scroll_options)
        try:
            _query = self._build_query(q, options=options, **kwargs)
            if aggs:
                _query['aggs'] = aggs
            if options.rawquery:
                return _query
            res = self._search(_query, options=options, **options.kwargs)
        except QueryError as e:
            msg = str(e)
            return {'success': False,
                    'error': msg}
        except RequestError as e:
            return {"error": "invalid query term: %s" % repr(e), "success": False}
        except NotFoundError as e:
            return {"error": e.error, "success": False}
        except Exception as e:
            logging.error("%s" % str(e))
            return {'success': False, 'error': "Something is wrong with query '%s'" % q}

        # if options.fetch_all:
        #     return res

        if not options.raw:
            res = self._cleaned_res2(res, options=options)
        return res

    def scroll(self, scroll_id, **kwargs):
        '''return the results from a scroll ID, recognizes options.raw'''
        options = self._get_cleaned_query_options(kwargs)
        try:
            r = self._es.scroll(scroll_id, scroll=self._scroll_time)
        except (NotFoundError, RequestError, TransportError):
            return {'success': False, 'error': 'Invalid or stale scroll_id.'}
        scroll_id = r.get('_scroll_id')
        if scroll_id is None or not r['hits']['hits']:
            return {'success': False, 'error': 'No results to return.'}
        else:
            if not options.raw:
                res = self._cleaned_res2(r, options=options)
            #res.update({'_scroll_id': scroll_id})
            if r['_shards']['failed']:
                res.update({'_warning': 'Scroll request has failed on {} shards out of {}.'.format(r['_shards']['failed'], r['_shards']['total'])})
        return res

    def _get_mapping(self, **kwargs):
        options = kwargs.pop('options', {})
        return self._es.indices.get_mapping(**kwargs)

    def _populate_metadata(self):
        ''' override to load metadata into ES mapping if it doesn't exist '''
        return {}

    def get_mapping_meta(self, **kwargs):
        """ return the current _meta field."""
        options = self._get_cleaned_metadata_options(kwargs)
        m = self._get_mapping(index=self._index, doc_type=self._doc_type, options=options, **kwargs)
        m = m[list(m.keys())[0]]['mappings'][self._doc_type]
        m = m.get('_meta', {})
        if m:
            return m
        else:
            # override to lazy-load metadata...
            return self._populate_metadata()

    def _get_fields(self, **kwargs):
        """ override for custom get_fields. """
        options = kwargs.pop('options', {})
        return self._es.indices.get(**kwargs)

    def query_fields(self, **kwargs):
        # query the metadata to get the available fields for a biothing object
        options = self._get_cleaned_metadata_options(kwargs)
        r = self._get_fields(index=self._index, options=options, **kwargs)
        return r[list(r.keys())[0]]['mappings'][self._doc_type]['properties']

    def status_check(self, bid):
        r = self.get_biothing(bid)
        return r


class ESQueryBuilder(object):
    def __init__(self, **query_options):
        self._query_options = query_options
        self._options = self._query_options.get('options', {})
            
    def build_id_query(self, bid, scopes=None):
        _default_scopes = '_id'
        scopes = scopes or _default_scopes
        if is_str(scopes):
            _query = {
                "match": {
                    scopes: {
                        "query": "{}".format(bid),
                        "operator": "and"
                    }
                }
            }
        elif is_seq(scopes):
            _query = {
                "multi_match": {
                    "query": "{}".format(bid),
                    "fields": scopes,
                    "operator": "and"
                }
            }
        else:
            raise ValueError('"scopes" cannot be "%s" type'.format(type(scopes)))
        _q = {"query": _query}
        self._query_options.pop("query", None)    # avoid "query" be overwritten by self.query_options
        _q.update(self._query_options)
        return _q

    def build_multiple_id_query(self, bid_list, scopes=None):
        """make a query body for msearch query."""
        _q = []
        for id in bid_list:
            _q.extend(['{}', json.dumps(self.build_id_query(id, scopes))])
        _q.append('')
        return '\n'.join(_q)
        
    def default_query(self, q):
        return json.loads(json.dumps({
            "query": {
                "query_string": {
                    "query": q.lstrip('*?')
                }
            }
        }))

    def user_query(self, q):
        args = {'q': q}
        args.update(self._options.userquery_kwargs)
        return json.loads(get_userquery(biothing_settings.userquery_dir, self._options.userquery).format(**args))

    def user_query_filter(self):
        return json.loads(get_userfilter(biothing_settings.userquery_dir, self._options.userquery))

    def extra_query_filters(self, filters):
        ''' Subclass to add extra filters '''
        return filters

    def get_query_filters(self):
        '''Subclass to add specific filters'''
        _filter = []
        if self._options.userquery and self.user_query_filter():
            _filter.append(self.user_query_filter())
        _filter = self.extra_query_filters(_filter)
        return _filter

    def add_query_filters(self, _query):
        '''filters added here will be applied in a filtered query,
           thus will affect the facet counts.
        '''
        filters = self.get_query_filters()
        if not filters:
            return _query

        #add filters as filtered query
        #this will apply to facet counts
        _query = {
            'filtered': {
                'query': _query,
                'filter': filters
            }
        }

        return _query

    def generate_query(self, q):
        '''
        Return query dict according to passed arg "q". Can be:
            - match query
            - wildcard query
            - raw_string query
            - "match all" query
        Also add query filters
        '''
        # Check if fielded/boolean query, excluding special goid query
        # raw_string_query should be checked ahead of wildcard query, as
        # raw_string may contain wildcard as well # e.g., a query
        # "symbol:CDK?", should be treated as raw_string_query.
        if self._is_user_query() and self.user_query(q):
            _query = self.user_query(q)
        elif q == '__all__':
            _query = {"match_all": {}}
        else:
            _query = self.default_query(q)

        _query = self.add_query_filters(_query)

        return _query

    def _is_user_query(self):
        ''' Return True if query is a userquery '''
        if self._options.userquery:
            return True
        return False

    def _is_wildcard_query(self, query):
        '''Return True if input query is a wildcard query.'''
        return query.find('*') != -1 or query.find('?') != -1

    def _is_raw_string_query(self, query):
        '''Return True if input query is a wildchar/fielded/boolean query.'''
        for v in [':', '~', ' AND ', ' OR ', 'NOT ']:
            if query.find(v) != -1:
                return True
        if query.startswith('"') and query.endswith('"'):
            return True
        return False

    def raw_string_query(self, q):
        _query = {
            "query_string": {
                "query": "%(q)s",
                # "analyzer": "string_lowercase",
                "default_operator": "AND",
                "auto_generate_phrase_queries": True
            }
        }
        _query = json.dumps(_query)
        try:
            _query = json.loads(_query % {'q': q.replace('"', '\\"')})
        except ValueError:
            raise QueryError("invalid query term.")
        return _query


    def wildcard_query(self, q):
        raise NotImplemented("Wildcard queries not supported (or implement in subclass)")


    def dis_max_query(self, q):
        raise NotImplemented("Dis max queries not supported (or implement in subclass)")

