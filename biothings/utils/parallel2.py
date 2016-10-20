from biothings.utils.backend import DocESBackend, DocMongoBackend, DocMemoryBackend, DocBackendOptions
from biothings.utils.es import ESIndexer
from biothings.utils.common import iter_n
from multiprocessing import Pool
from glob import glob
from os.path import abspath, isdir, join
from os import cpu_count

# How many cpus on this machine?
DEFAULT_THREADS_WITHOUT_MASTER = cpu_count()
DEFAULT_THREADS_WITH_MASTER = DEFAULT_THREADS_WITHOUT_MASTER
if DEFAULT_THREADS_WITHOUT_MASTER > 1:
    DEFAULT_THREADS_WITHOUT_MASTER -= 1

# simple aggregation functions
def agg_by_sum(prev, curr):
    return prev + curr

def agg_by_append(prev, curr):
    if isinstance(curr, list):
        return prev + curr
    return prev + [curr]

# avoid the global variable and the callback function this way
class ParallelResult(object):
    def __init__(self, agg_function, agg_function_init):
        self.res = agg_function_init
        self.agg_function = agg_function

    def aggregate(self, curr):
        self.res = self.agg_function(self.res, curr)

# Handles errors in async apply
class ErrorHandler(object):
    def __init__(self, errpath, chunk_num):
        if errpath:
            self.error_file_path = errpath + '_{}'.format(chunk_num)
        else:
            self.error_file_path = None

    def handle(self, exception):
        if self.error_file_path:
            f = open(self.error_file_path, 'w')
            f.write('{}\n'.format(exception))
            f.close()
        pass

def run_parallel_by_ids_file(fun, ids_file, backend_options=None, agg_function=agg_by_append, agg_function_init=[],
                            chunk_size=1000000, num_workers=DEFAULT_THREADS_WITH_MASTER, outpath=None, 
                            mget_chunk_size=10000, ignore_None=True, error_path=None, **query_kwargs):
    ''' Basically the same as run_parallel_by_query, but using a list of ids in a file instead of a query result. '''
    # Initialize return type
    ret = ParallelResult(agg_function, agg_function_init)

    # assert backend_options is correct
    if not backend_options or not isinstance(backend_options, DocBackendOptions):
        raise Exception("backend_options must be a biothings.databuild.parallel2.DocBackendOptions class")

    # build backend from options
    backend = backend_options.cls.create_from_options(backend_options)

    if not ids_file:
        raise Exception("ids_file must be a path to a file with ids, one per line")

    ids_file = abspath(ids_file)

    # normalize path for out files
    if outpath:
        outpath = abspath(outpath)

    if error_path:
        error_path = abspath(error_path)

    chunk_num = 0
    
    with open(ids_file, 'r') as ids_handle, Pool(processes=num_workers) as p:
        for (chunk_num, chunk) in enumerate(iter_n(_file_iterator(ids_handle), chunk_size)):
            # apply function to chunk
            p.apply_async(_run_one_chunk_ids_list, 
                    args=(chunk_num, chunk, fun, backend_options, agg_function, agg_function_init, 
                    outpath, mget_chunk_size, ignore_None), callback=ret.aggregate, 
                    error_callback=ErrorHandler(error_path, chunk_num).handle)
        # close pool and wait for completion of all workers
        p.close()
        p.join()
    return ret.res

# TODO: allow mget args to be passed
def run_parallel_by_query(fun, backend_options=None, query=None, agg_function=agg_by_append, 
                        agg_function_init=[], chunk_size=1000000, num_workers=DEFAULT_THREADS_WITH_MASTER, 
                        outpath=None, mget_chunk_size=10000, ignore_None=True, error_path=None, **query_kwargs):
    ''' This function will run a user function on all documents in a backend database in parallel using
        multiprocessing.Pool.  The overview of the process looks like this:  

        Chunk (into chunks of size "chunk_size") documents in the results of "query" on backend 
        (specified by "backend_options"), and run the following script on each chunk using a 
        multiprocessing.Pool object with "num_workers" processes:
            For each document in list of ids in this chunk (documents retrived in chunks of "mget_chunk_size"):
                Run function "fun" with parameters 
                (doc, chunk_num, f <file handle only passed if "outpath" is not None>),
                and aggregate the result with the current results using function "agg_function".
        
        :param fun:             
                The function to run on all documents.  If outpath is NOT specified, fun
                must accept two parameters: (doc, chunk_num), where doc is the backend document,
                and chunk_num is essentially a unique process id.
                If outpath IS specified, an additional open file handle (correctly tagged with
                the current chunk's chunk_num) will also be passed to fun, and thus it
                must accept three parameters: (doc, chunk_num, f)
        :param backend_options:
                An instance of biothings.utils.backend.DocBackendOptions.  This contains 
                the options necessary to instantiate the correct backend class (ES, mongo, etc).
        :param query:
                This query specifies the set of documents to be analyzed in parallel.  Defaults
                to match all.
        :param agg_function:
                This function aggregates the return value of each run of function fun.  It should take
                2 parameters: (prev, curr), where prev is the previous aggregated result, and curr
                is the output of the current function run.  It should return some value that represents
                the aggregation of the previous aggregated results with the output of the current 
                function.
        :param agg_function_init:
                Initialization value for the aggregated result.
        :param chunk_size:
                Length of the ids list sent to each chunk.
        :param num_workers:
                Number of processes that consume chunks in parallel.
                https://docs.python.org/2/library/multiprocessing.html#multiprocessing.pool.multiprocessing.Pool  
        :param outpath:
                Base path for output files.  Because function fun can be run many times in parallel, each
                chunk is sequentially numbered, and the output file name for any chunk is outpath_{chunk_num},
                e.g., if outpath is out, all output files will be of the form: /path/to/cwd/out_1, 
                /path/to/cwd/out_2, etc.
        :param error_path:
                Base path for error files.  If included, exceptions inside each chunk thread will be printed to
                these files.
        :param mget_chunk_size:
                The size of each mget chunk inside each chunk thread.  In each thread, the ids list
                is consumed by passing chunks to a mget_by_ids function.  This parameter controls the size of 
                each mget.
        :param ignore_None:
                If set, then falsy values will not be aggregated (0, [], None, etc) in the aggregation step.
                Default True.

        All other parameters are fed to the backend query.
      '''
   
    # Initialize return type
    ret = ParallelResult(agg_function, agg_function_init)

    # assert backend_options is correct
    if not backend_options or not isinstance(backend_options, DocBackendOptions):
        raise Exception("backend_options must be a biothings.databuild.parallel2.DocBackendOptions class")

    # build backend from options
    backend = backend_options.cls.create_from_options(backend_options)

    # normalize path for out files
    if outpath:
        outpath = abspath(outpath)

    if error_path:
        error_path = abspath(error_path)

    # Initialize pool
    with Pool(processes=num_workers) as p:
        for (chunk_num, chunk) in enumerate(iter_n(backend.query(query, _source=False, **query_kwargs), chunk_size)):
            # apply function to chunk
            p.apply_async(_run_one_chunk_ids_list, 
                    args=(chunk_num, chunk, fun, backend_options, agg_function, agg_function_init, 
                    outpath, mget_chunk_size, ignore_None), callback=ret.aggregate, 
                    error_callback=ErrorHandler(error_path, chunk_num).handle)
            #p.starmap_async(_run_one_chunk_ids_list, 
            #    _create_iterator(backend.query(query, _source=False, **query_kwargs)),
            #    chunksize=chunk_size, callback=ret.aggregate) #
        p.close()
        p.join()
    return ret.res

def run_parallel_by_ids_dir(fun, ids_dir, backend_options=None, agg_function=agg_by_append, 
                        agg_function_init=[], chunk_size=1000000, outpath=None
                        num_workers=DEFAULT_THREADS_WITHOUT_MASTER, mget_chunk_size=10000, 
                        ignore_None=True, error_path=None, **query_kwargs):
    ''' This function will run function fun on chunks defined by the files in ids_dir.
        
    '''
    # Initialize return type
    ret = ParallelResult(agg_function, agg_function_init)

    # assert backend_options is correct
    if not backend_options or not isinstance(backend_options, DocBackendOptions):
        raise Exception("backend_options must be a biothings.databuild.parallel2.DocBackendOptions class")

    # build backend from options
    backend = backend_options.cls.create_from_options(backend_options)

    # normalize path for directory containing id files
    ids_dir = abspath(ids_dir)
    assert isdir(ids_dir)
    # get the path to the files
    files = enumerate(glob.glob(join(ids_dir), '*'))
 
    # normalize path for out files, if requested
    if outpath:
        outpath = abspath(outpath)
    
    # and error files, if requested
    if error_path:
        error_path = abspath(error_path)

    with Pool(processes=num_workers) as p:
        pass

    return ret.res 

def _run_one_chunk_ids_list(chunk_num, chunk, fun, backend_options, agg_function, 
                            agg_function_init, outpath, mget_chunk_size, ignore_None):
    # recreate backend object
    backend = backend_options.cls.create_from_options(backend_options)
   
    # make file handle for this chunk
    if outpath:
        _file = open(outpath + '_{}'.format(chunk_num), 'w')
    
    # initialize return for this chunk
    ret = ParallelResult(agg_function, agg_function_init)
    
    # iterate through this chunk and run function on every doc
    for doc in backend.mget_from_ids(chunk, step=mget_chunk_size):
        # Actually call the function
        if outpath:
            r = fun(doc, chunk_num, _file)
        else:
            r = fun(doc, chunk_num)

        # aggregate the results
        if r or not ignore_None:
            ret.aggregate(r)
    
    # close handle
    if outpath and _file:
        _file.close()
    return ret.res

def _file_iterator(chunk_file):
    for line in chunk_file:
        yield line.strip('\n')

"""def _run_one_chunk_ids_dir(chunk_num, chunk_path, fun, backend, agg_function, agg_function_init, outpath, mget_chunk_size, ignore_None):


    if isinstance(chunk, str) and os.path.exists(chunk):
        chunk_file = open(chunk, 'r')
        iterator = _file_iterator(chunk_file)
    else:
        chunk_file = None
        iterator = backend.mget_from_ids(chunk, step=mget_chunk_size)
    

    this_chunk = []
    this_chunk_len = 0
    ret = agg_function_init
    for this_id in iterator:
        this_chunk.append(this_id)
        this_chunk_len += 1
        if this_chunk_len < mget_chunk_size:
            continue
        # Chunk full, get the docs from ES with mget and continue this chunk
        chunk_res = backend.mget_from_ids(this_chunk, mget_chunk_size)
        this_chunk_len = 0
        this_chunk = []
        if 'docs' not in chunk_res:
            continue
        ret_dict = _process_chunk(chunk_num, chunk_res, test_conf, files, ret_dict)
    # do partial chunks
    if this_chunk:
        chunk_res = _get_chunk(this_chunk, es, test_conf)
        this_chunk = []
        if 'docs' in chunk_res:
            ret_dict = _process_chunk(chunk_num, chunk_res, test_conf, files, ret_dict)
    # close files
    _close_file_struct(files)
    if chunk_file:
        chunk_file.close()
    return ret_dict

def _process_chunk(chunk_num, chunk_res, test_conf, files, ret_dict):
    for doc in chunk_res['docs']:
        if (('found' not in doc) or (('found' in doc) and not doc['found'])):
            continue
        for (index, test) in enumerate(test_conf.tests):
            # Run this test on the doc
            if index in files:
                test_ret = test.f(doc, chunk_num, files[index])
            else:
                test_ret = test.f(doc, chunk_num)
            if test_ret and test.ret_key:
                # TODO: Maybe should allow an aggregation function...
                ret_dict.setdefault(test.ret_key, []).append(test_ret)
    return ret_dict

def _chunk_iterator(chunk_list):
    for tid in chunk_list:
        yield tid

"""