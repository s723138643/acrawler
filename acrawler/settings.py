import json
import pathlib

DEFAULT_CONFIG = {
        # 设置默认class
        'EngineClass': 'acrawler.engine.Engine',
        'SchedulerClass': 'acrawler.scheduler.Scheduler',
        'FilterClass': 'acrawler.filterlib.memfilter.MemFilter',
        'QueueClass': 'acrawler.queuelib.sqlitequeue.PrioritySQLiteQueue',
        # 以下是各模块设置
        'spider': {
            'headers': {
                'User-Agent': (
                    'Mozilla/5.0 (X11; Linux x86_64; rv:42.0)'
                    ' Gecko/20100101 Firefox/43.0'),
                },
            'save_cookie': False,
            'debug': False,
            },
        'engine': {
            'threads': 1
            },
        'scheduler': {
            'queue':{
                'sqlite_path': './task'
            },
            'filter': {
                'hostonly': True,
                'maxredirect': None,        # 默认不设置最大跳转数
                'maxdeep': None,            # 默认不设置最大爬取深度
                'db_path': './filter'
                }
            }
        }


def get_settings_from_file(path):
    settings = DEFAULT_CONFIG
    p = pathlib.Path(path)
    if p.is_file():
        with p.open('r') as f:
            tmp = json.load(f)
        merge_settings(settings, tmp)
    return settings

def merge_settings(default, current):
    for key in default:
        if key in current:
            if isinstance(default[key], dict):
                if isinstance(current[key], dict):
                    merge_settings(default[key], current[key])
            else:
                if not isinstance(current[key], dict):
                    default[key] = current[key]
    return default

def make_config(path):
    settings = DEFAULT_CONFIG
    p = pathlib.Path(path)
    with p.open('w') as f:
        json.dump(settings, f, indent=True)


if __name__ == '__main__':
    print(type(DEFAULT_CONFIG))
    print(DEFAULT_CONFIG)
    tmp = get_settings_from_file('config.json')
    print(tmp)
    print(type(tmp))
