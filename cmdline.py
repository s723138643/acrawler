import argparse
import logging
import importlib

from .settings import get_settings_from_file, DEFAULT_CONFIG
from .settings import make_config


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
            dest='config', default='config.json',
            help='specify the config file to use')
    parser.add_argument(
            '-m', '--make',
            dest='make', action='store_true',
            help='create the default config file "config.json", '
                 'and exit'
            )
    parser.add_argument(
            '-d', '--debug',
            dest='debug', action="store_true",
            help='enable debug mode')

    return parser.parse_args()


def split_moduler(moduler):
    assert type(moduler) is str, 'moduler is not a str'
    x = moduler.split('.')
    m = '.'.join(x[:-1])
    return m, x[-1]


def execute(SpiderClass, *, SchedulerClass=None,
            QueueClass=None, FilterClass=None, EngineClass=None):
    args = get_args()
    if args.make:
        make_config('./config.json')
        return
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

    # import modules
    if SchedulerClass is None:
        moduler, name = split_moduler(settings['SchedulerClass'])
        m = importlib.import_module(moduler)
        SchedulerClass = getattr(m, name)
    if QueueClass is None:
        moduler, name = split_moduler(settings['QueueClass'])
        m = importlib.import_module(moduler)
        QueueClass = getattr(m, name)
    if FilterClass is None:
        moduler, name = split_moduler(settings['FilterClass'])
        m = importlib.import_module(moduler)
        FilterClass = getattr(m, name)
    if EngineClass is None:
        moduler, name = split_moduler(settings['EngineClass'])
        m = importlib.import_module(moduler)
        EngineClass = getattr(m, name)

    if args.threads and args.threads > 0:
        settings['engine']['threads'] = args.threads

    engine = EngineClass(settings, SpiderClass, SchedulerClass,
                         QueueClass, FilterClass, resume=args.resume)
    engine.run()

if __name__ == '__main__':
    print(get_args())
