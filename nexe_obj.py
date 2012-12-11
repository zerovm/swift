from sys import argv, exit
import re
import logging
import cPickle as pickle
from time import sleep
from argparse import ArgumentParser

def errdump(zvm_errcode, nexe_validity, nexe_errcode, nexe_etag, nexe_accounting, status_line):
    print '%d\n%d\n%s\n%s\n%s' % (nexe_validity, nexe_errcode, nexe_etag,
                                  ' '.join([str(val) for val in nexe_accounting]), status_line)
    exit(zvm_errcode)

parser = ArgumentParser()
parser.add_argument('-M', dest='manifest')
parser.add_argument('-s', action='store_true', dest='skip')
parser.add_argument('-z', action='store_true', dest='validate')
args = parser.parse_args()

valid = 1
if args.skip:
    valid = 0
accounting = [0,0,0,0,0,0,0,0,0,0,0,0]
manifest = args.manifest
if not manifest:
    errdump(1,valid, 0,'',accounting,'Manifest file required')
try:
    inputmnfst = file(manifest, 'r').read().splitlines()
except IOError:
    errdump(1,valid, 0,'',accounting,'Cannot open manifest file: %s' % manifest)
dl = re.compile("\s*=\s*")
mnfst_dict = dict()
for line in inputmnfst:
    (attr, val) = re.split(dl, line, 1)
    if attr and attr in mnfst_dict:
        mnfst_dict[attr] += ',' + val
    else:
        mnfst_dict[attr] = val

class Mnfst:
    pass

mnfst = Mnfst()
index = 0
status = 'nexe did not run'
retcode = 0

def retrieve_mnfst_field(n, eq=None, min=None, max=None, isint=False, optional=False):
    if n not in mnfst_dict:
        if optional:
            return
        errdump(1,valid,0,'',accounting,'Manifest key missing "%s"' % n)
    v = mnfst_dict[n]
    if isint:
        v = int(v)
        if min and v < min:
            errdump(1,valid,0,'',accounting,'%s = %d is less than expected: %d' % (n,v,min))
        if max and v > max:
            errdump(1,valid,0,'',accounting,'%s = %d is more than expected: %d' % (n,v,max))
    if eq and v != eq:
        errdump(1,valid,0,'',accounting,'%s = %s and expected %s' % (n,v,eq))
    setattr(mnfst, n.strip(), v)


retrieve_mnfst_field('Version', '09082012')
retrieve_mnfst_field('Nexe')
exe = file(mnfst.Nexe, 'r').read()
if 'INVALID' == exe:
    valid = 2
    retcode = 0
if args.validate:
    print '%d\n%d\n%s\n%s\n%s' % (valid, retcode, '',
                                  ' '.join([str(val) for val in accounting]), status)
    exit(0)
retrieve_mnfst_field('NexeMax', isint=True)
retrieve_mnfst_field('SyscallsMax', min=1, isint=True)
retrieve_mnfst_field('NexeEtag', optional=True)
retrieve_mnfst_field('Timeout', min=1, isint=True)
retrieve_mnfst_field('MemMax', min=32*1048576, max=4096*1048576, isint=True)
retrieve_mnfst_field('Environment', optional=True)
retrieve_mnfst_field('CommandLine', optional=True)
retrieve_mnfst_field('Channel')
retrieve_mnfst_field('NodeName', optional=True)
retrieve_mnfst_field('NameServer', optional=True)

channel_list = re.split('\s*,\s*',mnfst.Channel)
if len(channel_list) % 7 != 0:
    errdump(1,valid,0,mnfst.Nexe,accounting,'wrong channel config: %s' % mnfst.Channel)
dev_list = channel_list[1::7]
for i in xrange(0,len(dev_list)):
    device = dev_list[i]
    fname = channel_list[i*7]
    if device == '/dev/stdin' or device == '/dev/input' or device == '/dev/cdr':
        mnfst.input = fname
    elif device == '/dev/stdout' or device == '/dev/output':
        mnfst.output = fname
    elif device == '/dev/stderr':
        logging.basicConfig(filename=fname,level=logging.DEBUG,filemode='w')
if valid < 2:
    try:
        inf = file(mnfst.input, 'r')
        ouf = file(mnfst.output, 'w')
        ins = inf.read()
        accounting[4] += 1
        accounting[5] += len(ins)
        id = pickle.loads(ins)
    except EOFError:
        id = []
    except Exception:
        errdump(1,valid,0,mnfst.Nexe,accounting,'Std files I/O error')

    od = ''
    try:
        od = pickle.dumps(eval(exe))
    except Exception:
        logging.exception('Exception:')
    ouf.write(od)
    accounting[6] += 1
    accounting[7] += len(od)
    inf.close()
    ouf.close()
    status = 'ok.'
print '%d\n%d\n%s\n%s\n%s' % (valid, retcode, mnfst.NexeEtag,
                              ' '.join([str(val) for val in accounting]), status)
logging.info('finished')
exit(0)