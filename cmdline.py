import pathlib
import argparse
import logging

from .engine import Engine
from .sfilter import BlumeFilter
from .scheduler import Scheduler
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

def execute(
        SpiderClass, *,
        SchedulerClass=Scheduler,
        FilterClass=BlumeFilter):
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
        filter_path = settings.get('filter').get('db_path')
        blumefile = settings.get('filter').get('blumedb')
        sqlite3file = settings.get('filter').get('sqlitedb')
        p = pathlib.Path(filter_path)
        blume = p / blumefile
        sqlitedb = p / sqlite3file
        if blume.is_file():
            blume.unlink()
        if sqlitedb.is_file():
            sqlitedb.unlink()
        task_path = settings.get('scheduler').get('task_path')
        task_name = settings.get('scheduler').get('task_name')
        p = pathlib.Path(task_path)
        for child in p.iterdir():
            if child.is_file() and child.match(task_name+'_*'):
                child.unlink()
    if args.threads and args.threads > 0:
        settings['engine']['threads'] = args.threads
    engine = Engine(settings, SpiderClass, SchedulerClass, FilterClass)
    engine.run()

if __name__ == '__main__':
    print(get_args())
