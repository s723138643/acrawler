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
    parser.add_argument(
            '-w', '--logfile',
            dest='logfile', default=None,
            help='write log to logfile')

    return parser.parse_args()


def load_obj(path):
    assert isinstance(path, str), ValueError('excepted a str object')
    try:
        dot = path.rindex('.')
    except IndexError:
        raise ValueError('"load_obj" excepted a full path')
    module_str, cls_str = path[:dot], path[dot+1:]
    module = importlib.import_module(module_str)
    return getattr(module, cls_str)


def set_logging(level, logfile=None):
    confdict = {
        'format': '[%(asctime)s] %(name)s-%(levelname)s:: %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S',
        'level': level
    }
    if logfile:
        confdict['filename'] = logfile

    logging.basicConfig(**confdict)
    logging.getLogger('Engine').setLevel(level)
    logging.getLogger('Scheduler').setLevel(level)
    logging.getLogger('Scheduler.Filter').setLevel(level)
    logging.getLogger('Scheduler.Queue').setLevel(level)
    logging.getLogger('Spider').setLevel(level)

    if level == logging.DEBUG:
        logging.warn(
            'debug mode enabled, when error occured spider'
            ' will stop immediately'
            )


def execute(SpiderClass, *, SchedulerClass=None,
            QueueClass=None, FilterClass=None,
            EngineClass=None):
    args = get_args()

    if args.make:
        make_config('./config.json')
        return

    if args.config:
        settings = get_settings_from_file(args.config)
    else:
        settings = DEFAULT_CONFIG

    if args.debug:
        set_logging(logging.DEBUG, args.logfile)
        settings['spider']['debug'] = True
    else:
        set_logging(logging.INFO, args.logfile)

    # import modules
    if SchedulerClass is None:
        SchedulerClass = load_obj(settings['SchedulerClass'])
    if QueueClass is None:
        QueueClass = load_obj(settings['QueueClass'])
    if FilterClass is None:
        FilterClass = load_obj(settings['FilterClass'])
    if EngineClass is None:
        EngineClass = load_obj(settings['EngineClass'])

    if args.threads and args.threads > 0:
        settings['engine']['threads'] = args.threads

    engine = EngineClass(settings,
                         SpiderClass, SchedulerClass,
                         QueueClass, FilterClass,
                         resume=args.resume)
    engine.run()

if __name__ == '__main__':
    print(get_args())
