import argparse
import logging

from .engine import Engine
from .sfilter import BlumeFilter
from .scheduler import Scheduler
from .squeue import PrioritySQLiteQueue
from .settings import get_settings_from_file, DEFAULT_CONFIG

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '-t', '--threads',
            dest='threads', type=int,
            help='specify the number of spider to work')
    parser.add_argument(
            '-f', '--force',
            dest='resume', action='store_false',
            help='ignore task remaind in queue, start a new task')
    parser.add_argument(
            '-c', '--config',
            dest='config', default='',
            help='specify the config file to use')
    parser.add_argument(
            '-d', '--debug',
            dest='debug', action="store_true",
            help='enable debug mode')

    return parser.parse_args()

def execute(SpiderClass, *, SchedulerClass=Scheduler,
            QueueClass=PrioritySQLiteQueue, FilterClass=BlumeFilter):
    args = get_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug('enable debug mode, set logger Level to DEBUG')
        logging.getLogger('Engine').setLevel(logging.DEBUG)
        logging.getLogger('Scheduler').setLevel(logging.DEBUG)
        logging.getLogger('Filter').setLevel(logging.DEBUG)
        logging.getLogger('Spider').setLevel(logging.DEBUG)
    if args.config:
        settings = get_settings_from_file(args.config)
    else:
        settings = DEFAULT_CONFIG
    if not args.resume:
        FilterClass.clean(settings['scheduler']['filter'])
        QueueClass.clean(settings['scheduler']['queue'])
    if args.threads and args.threads > 0:
        settings['engine']['threads'] = args.threads
    engine = Engine(settings, SpiderClass, SchedulerClass,
                    QueueClass, FilterClass)
    engine.run(resume=args.resume)

if __name__ == '__main__':
    print(get_args())
