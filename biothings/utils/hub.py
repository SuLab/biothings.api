# from http://asyncssh.readthedocs.io/en/latest/#id13

# To run this program, the file ``ssh_host_key`` must exist with an SSH
# private key in it to use as a server host key.

import os, glob, re, pickle, datetime
import asyncio, asyncssh, crypt, sys, io
import types, aiocron, time
from functools import partial
from IPython import InteractiveShell
import psutil
from pprint import pprint
from collections import OrderedDict

from biothings import config
from biothings.utils.common import timesofar, sizeof_fmt

# useful variables to bring into hub namespace
pending = "pending"
done = "done"


##############
# HUB SERVER #
##############

class HubSSHServerSession(asyncssh.SSHServerSession):
    def __init__(self, name, commands):
        self.shell = InteractiveShell(user_ns=commands)
        self.name = name
        self._input = ''

    def connection_made(self, chan):
        self._chan = chan
        self.origout = sys.stdout
        self.buf = io.StringIO()
        sys.stdout = self.buf

    def shell_requested(self):
        return True

    def session_started(self):
        self._chan.write('\nWelcome to %s, %s!\n' % (self.name,self._chan.get_extra_info('username')))
        self._chan.write('hub> ')

    def data_received(self, data, datatype):
        self._input += data

        lines = self._input.split('\n')
        for line in lines[:-1]:
            if not line:
                continue
            self.origout.write("run %s " % repr(line))
            r = self.shell.run_code(line)
            if r == 1:
                self.origout.write("Error\n")
                etype, value, tb = self.shell._get_exc_info(None)
                self._chan.write("Error: %s\n" % value)
            else:
                #self.origout.write(self.buf.read() + '\n')
                self.origout.write("OK\n")
                self.buf.seek(0)
                self._chan.write(self.buf.read())
                # clear buffer
                self.buf.seek(0)
                self.buf.truncate()
        self._chan.write('hub> ')
        self._input = lines[-1]

    def eof_received(self):
        self._chan.write('Have a good one...\n')
        self._chan.exit(0)

    def break_received(self, msec):
        # simulate CR
        self._chan.write('\n')
        self.data_received("\n",None)


class HubSSHServer(asyncssh.SSHServer):

    COMMANDS = {}
    PASSWORDS = {}

    def session_requested(self):
        return HubSSHServerSession(self.__class__.NAME,
                                   self.__class__.COMMANDS)

    def connection_made(self, conn):
        print('SSH connection received from %s.' %
                  conn.get_extra_info('peername')[0])

    def connection_lost(self, exc):
        if exc:
            print('SSH connection error: ' + str(exc), file=sys.stderr)
        else:
            print('SSH connection closed.')

    def begin_auth(self, username):
        # If the user's password is the empty string, no auth is required
        return self.__class__.PASSWORDS.get(username) != ''

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        pw = self.__class__.PASSWORDS.get(username, '*')
        return crypt.crypt(password, pw) == pw


async def start_server(loop,name,passwords,keys=['bin/ssh_host_key'],
                        host='',port=8022,commands={}):
    for key in keys:
        assert os.path.exists(key),"Missing key '%s' (use: 'ssh-keygen -f %s' to generate it" % (key,key)
    HubSSHServer.PASSWORDS = passwords
    HubSSHServer.NAME = name
    if commands:
        HubSSHServer.COMMANDS.update(commands)
    await asyncssh.create_server(HubSSHServer, host, port, loop=loop,
                                 server_host_keys=keys)


####################
# DEFAULT HUB CMDS #
####################
# these can be used in client code to define
# commands. partial should be used to pass the
# required arguments, eg.:
# {"schedule" ; partial(schedule,loop)}

class JobRenderer(object):

    def __init__(self):
        self.rendered = {
                types.FunctionType : self.render_func,
                types.MethodType : self.render_method,
                partial : self.render_partial,
                types.LambdaType: self.render_lambda,
        }

    def render(self,job):
        r = self.rendered.get(type(job._callback))
        rstr = r(job._callback)
        delta = job._when - job._loop.time()
        strdelta = time.strftime("%Hh:%Mm:%Ss", time.gmtime(int(delta)))
        return "%s {run in %s}" % (rstr,strdelta)

    def render_partial(self,p):
        # class.method(args)
        return self.rendered[type(p.func)](p.func) + "%s" % str(p.args)

    def render_cron(self,c):
        # func type associated to cron can vary
        return self.rendered[type(c.func)](c.func) + " [%s]" % c.spec

    def render_func(self,f):
        return f.__name__

    def render_method(self,m):
        # what is self ? cron ?
        if type(m.__self__) == aiocron.Cron:
            return self.render_cron(m.__self__)
        else:
            return "%s.%s" % (m.__self__.__class__.__name__,
                              m.__name__)

    def render_lambda(self,l):
        return l.__name__

renderer = JobRenderer()

def schedule(loop):
    jobs = {}
    # try to render job in a human-readable way...
    for sch in loop._scheduled:
        if type(sch) != asyncio.events.TimerHandle:
            continue
        if sch._cancelled:
            continue
        try:
            info = renderer.render(sch)
            print(info)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(sch)
    if len(loop._scheduled):
        print()

def find_process(pid):
    g = psutil.process_iter()
    for p in g:
        if p.pid == pid:
            break
    return p

def top(pqueue,pid=None):

    def extract_worker_info(worker):
        info = OrderedDict()
        proc = worker.get("process")
        info["pid"] = proc and proc.pid
        info["name"] = worker.get("func_name"," ")
        args = worker.get("args")
        info["arg1"] = ""
        if args:
            info["arg1"] = type(args) == str and args or str(args[0])
        info["mem"] = proc and sizeof_fmt(proc.memory_info().rss)
        info["cpu"] = proc and "%.1f%%" % proc.cpu_percent()
        info["started_at"] = worker.get("started_at")
        if worker.get("duration"):
            info["duration"] = worker["duration"]
        else:
            info["duration"] = timesofar(worker.get("started_at",0))
        info["files"] = []
        if proc:
            for pfile in proc.open_files():
                # skip 'a' (logger)
                if pfile.mode == 'r':
                    finfo = OrderedDict()
                    finfo["path"] = pfile.path
                    finfo["read"] = sizeof_fmt(pfile.position)
                    size = os.path.getsize(pfile.path)
                    finfo["size"] = sizeof_fmt(size)
                    info["files"].append(finfo)

        return info

    def extract_pending_info(pending):
        info = OrderedDict()
        num,pend = pending
        info["num"] = num
        info["name"] = pend.fn.__name__
        r = renderer.rendered[type(workitem.fn)]
        rstr = r(workitem.fn)
        return info

    def print_workers(workers):
        print("%d running job(s)" % len(workers))
        if workers:
            print("{0:<7} | {1:<36} | {2:<8} | {3:<8} | {4:<10}".format("pid","info","mem","cpu","time"))
            for pid in workers:
                worker = workers[pid]
                info = extract_worker_info(worker)
                try:
                    print('{pid:>7} | {name:>15} {arg1:>20} | {mem:>8} | {cpu:>8} | {duration:>10}'.format(**info))
                except TypeError:
                    pprint(info)

    def print_done(job_files):
        if job_files:
            print("{0:<36} | {1:<22} | {2:<10}".format("info","start","time"))
            for jfile in job_files:
                worker = pickle.load(open(jfile,"rb"))
                info = extract_worker_info(worker)
                # format start time
                tt = datetime.datetime.fromtimestamp(info["started_at"]).timetuple()
                info["started_at"] = time.strftime("%Y/%m/%d %H:%M:%S",tt)
                try:
                    print("{name:>36} | {started_at:>22} | {duration:<10}".format(**info))
                except TypeError as e:
                    print(e)
                    pprint(info)
                os.unlink(jfile)



    def print_detailed_worker(worker):
        info = extract_worker_info(worker)
        pprint(info)

    def get_pid_files(children,child):
        pat = re.compile(".*/(\d+)\.pickle")
        children_pids = [p.pid for p in children]
        pids = {}
        for fn in glob.glob(os.path.join(config.RUN_DIR,"*.pickle")):
            try:
                pid = int(pat.findall(fn)[0])
                if not pid in children_pids:
                    print("Removing staled pid file '%s'" % fn)
                    os.unlink(fn)
                else:
                    if not child or child.pid == pid:
                        worker = pickle.load(open(fn,"rb"))
                        worker["process"] = children[children_pids.index(pid)]
                        pids[pid] = worker
            except IndexError:
                # weird though... should have only pid files there...
                pass
        return pids

    def print_pending_info(num,pending):
        r = renderer.rendered[type(pending.fn.func)]
        rstr = r(pending.fn.func)
        print("{0:<4} | {1:<36}".format(num,rstr))

    def get_pending_summary(running,pqueue,getstr=False):
        return "%d pending job(s)" % (len(pqueue._pending_work_items) - running)

    def print_pendings(running,pqueue):
        # pendings are kept in queue while running, until result is there so we need
        # to adjust the actual real pending jobs. also, pending job are get() from the
        # queue following FIFO order. finally, worker ID is incremental. So...
        pendings = sorted(pqueue._pending_work_items.items())
        actual_pendings = pendings[running:]
        print(get_pending_summary(running,pqueue))
        if actual_pendings:
            print("{0:>4} | {1:>36}".format("id","info"))
            for num,pending in pendings[running:]:
                print_pending_info(num,pending)
            print()

    try:
        # get process children attached to hub pid
        phub = find_process(os.getpid())
        pchildren = phub.children()
        child = None
        pending = False
        done = False
        if pid:
            try:
                pid = int(pid)
                child = [p for p in pchildren if p.pid == pid][0]
            except ValueError:
                if pid == "pending":
                    pending = True
                elif pid == "done":
                    done = True
                else:
                    raise
        workers = get_pid_files(pchildren,child)
        done_jobs = glob.glob(os.path.join(config.RUN_DIR,"done","*.pickle"))
        if child:
            print_detailed_worker(workers[child.pid])
        elif pending:
            print_pendings(len(workers),pqueue)
        elif done:
            print_done(done_jobs)
            print("%d finished job(s)" % len(done_jobs))
        else:
            print_workers(workers)
            print("%s, type 'top(pending)' for more" % get_pending_summary(len(workers),pqueue))
            if done_jobs:
                print("%s finished job(s), type 'top(done)' for more" % len(done_jobs))
        if child:
            return list(workers.values())[0]
        else:
            return workers
    except psutil.NoSuchProcess as e:
        print(e)

def stats(src_dump):
    pass

