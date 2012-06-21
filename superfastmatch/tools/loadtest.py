import os
import sys
import time
import datetime
import random
from multiprocessing import Process, Event, Value

import superfastmatch
import argparse

parser = argparse.ArgumentParser(description='Attempt to overload the superfastmatch server.')
parser.add_argument('--verbose', '-v', action='count', help='Print confirmation for each document submitted to the SuperFastMatch server.')
parser.add_argument('--timeout', '-t', action='store', type=int, default=15, help='Timeout, in seconds, to wait for a response from the SuperFastMatch server.')
parser.add_argument('--workers', '-w', action='store', type=int, default=15, help='The number of workers to use.')
parser.add_argument('--modulo', '-m', action='store', type=int, default=5, help='Modulus of the worker count, used to make some workers search for the same text.')
parser.add_argument('--mintextlen', action='store', type=int, default=900, help='Minimum length of the search text.')
parser.add_argument('--maxtextlen', action='store', type=int, default=15000, help='Maximum length of the search text.')
parser.add_argument('dirname', action='store', help='Path to the folder containing text files to use as search text.')
parser.add_argument('url', action='store', help='Base URL of the SuperFastMatch server.')
args = parser.parse_args()


Shutdown = Event()
Unleash = Event()
ReadyWorkers = Value('i', 0)
Successes = Value('i', 0)
Failures = Value('i', 0)


def slurp(path):
    with file(path) as inf:
        return inf.read().decode('utf-8')


def build_searchtext_corpus(dirname, modulo):
    document_texts = dict([(path, slurp(path))
                           for path in (os.path.join(dirname, filename)
                                        for (idx, filename) in enumerate(os.listdir(dirname))
                                        if idx % modulo == 0)
                           if os.path.isfile(path)])
    return document_texts


def run_random_search(workernum, sfm, corpus, textrange):
    def warn(msg):
        print >>sys.stderr, u"[{0}] {1}".format(workernum, msg)

    (minlen, maxlen) = textrange
    corpus_keys = corpus.keys()
    random_key_offset = random.randint(0, len(corpus_keys) - 1)
    filename = corpus_keys[random_key_offset]
    searchtext = corpus[filename]
    length = random.randint(args.mintextlen, args.maxtextlen)
    if length < len(searchtext):
        offset = random.randint(0, len(searchtext) - length - 1)
        searchtext = searchtext[offset:length]

    try:
        sfm.search(text=searchtext, url=filename)
        Successes.acquire()
        Successes.value += 1
        Successes.release()
        return 'S'
    except KeyboardInterrupt:
        return '-'
    except superfastmatch.SuperFastMatchError, e:
        Failures.acquire()
        Failures.value += 1
        Failures.release()
        if e.status in (502, '502'):
            warn("The SuperFastMatch server is down.")
        else:
            warn("Exception {2!r} caught while searching for {0} character string: {1!r}".format(len(searchtext), searchtext[:40] + '...', e))
        return 'F'
    except Exception, e:
        Failures.acquire()
        Failures.value += 1
        Failures.release()
        warn("Exception {2!r} caught while searching for {0} character string: {1!r}".format(len(searchtext), searchtext[:40] + '...', e))
        return 'F'
    except:
        Failures.acquire()
        Failures.value += 1
        Failures.release()
        warn("Untyped exception caught while searching for {0} character string: {1!r}".format(len(searchtext), searchtext[:40] + '...'))
        return 'F'


def go(url, dirname, statuschar, workernum, modulo, textrange):
    def info(msg):
        print u"[{0}] {1}".format(workernum, msg)

    try:
        info(u"Starting.")
        corpus = build_searchtext_corpus(dirname, (workernum + 1) % args.modulo + 1)
        info(u"Corpus built ({0} documents), waiting for other workers.".format(len(corpus)))
        statuschar.value = 'R'
        ReadyWorkers.acquire()
        ReadyWorkers.value += 1
        ReadyWorkers.release()
        Unleash.wait()
        sfm = superfastmatch.Client(url, parse_response=False, timeout=args.timeout)
        statuschar.value = 'W'
        while Shutdown.is_set() == False:
            status = run_random_search(workernum, sfm, corpus, textrange)
            statuschar.value = status

    except KeyboardInterrupt:
        info(u"Shutting down.")
        Shutdown.set()


def main():
    print u"Starting workers."
    sys.stdout.flush()
    statuses = [Value('c', '-') for _ in range(0, args.workers)]
    processes = [Process(target=go, args=[args.url, args.dirname, statuses[n], n, args.modulo, (args.mintextlen, args.maxtextlen)])
                 for n in range(0, args.workers)]
    for proc in processes:
        proc.start()

    while ReadyWorkers.value < args.workers:
        sys.stdout.write(u" ".join([s.value for s in statuses]))
        sys.stdout.write("\x1B[1G")
        sys.stdout.flush()
        time.sleep(0.5)
    print u"Unleashing workers."
    Unleash.set()
    sys.stdout.flush()

    unleash_time = datetime.datetime.now()
    while Shutdown.is_set() == False:
        try:
            time.sleep(0.5)
            duration = float((datetime.datetime.now() - unleash_time).seconds)
            if duration == 0:
                rate = "---"
            else:
                successes = float(Successes.value)
                rate = round(successes / duration, 2)
            rate_unit = "per second"
            sys.stdout.write(u"\x1B[0K{0} querys {1}\n".format(rate, rate_unit))
            sys.stdout.write(u"\x1B[0K{0} Successes, {1} Failures\n".format(Successes.value, Failures.value))
            sys.stdout.write(u" ".join([s.value for s in statuses]))
            sys.stdout.write("\x1B[1G\x1B[2A")
            sys.stdout.flush()
        except KeyboardInterrupt:
            Shutdown.set()

    while True:
        try:
            for proc in processes:
                proc.join()
            break
        except KeyboardInterrupt:
            print u"Shutting down."
            pass

    sys.stdout.flush()
    sys.stdout.write("\n\n\n")
    duration = float((datetime.datetime.now() - unleash_time).seconds)
    if duration == 0:
        rate = "---"
    else:
        successes = float(Successes.value)
        rate = round(successes / duration, 2)
    rate_unit = "per second"
    sys.stdout.write("\x1B[1G\x1B[0K")
    sys.stdout.write(u"{0} querys {1}\n".format(rate, rate_unit))
    sys.stdout.write("\x1B[1G\x1B[0K")
    sys.stdout.flush()
    print u"{0} Successes, {1} Failures".format(Successes.value, Failures.value)

    return 0


if __name__ == "__main__":
    sys.exit(main())

