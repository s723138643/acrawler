import json
import os.path

DEFAULT_CONFIG = {
        'spider': {
            'headers': {
                'User-Agent': (
                    'Mozilla/5.0 (X11; Linux x86_64; rv:42.0)'
                    ' Gecko/20100101 Firefox/43.0'),
                },
            'save_cookie': False,
            },
        'filter': {
            'hostonly': True,
            'maxredirect': None,        # 默认不设置最大跳转数
            'maxdeep': None,            # 默认不设置最大爬取深度
            'db_path': './filter',
            'blumedb': 'blume.db',
            'sqlitedb': 'sqlite.db'
            },
        'engine': {
            'threads': 1
            },
        'scheduler': {
            'max_size': 0,
            'task_path': './task',
            'task_name': 'task_priority'
            }
        }


def get_settings_from_file(path):
    settings = DEFAULT_CONFIG
    if os.path.isfile(path):
        with open(path, 'r') as f:
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

if __name__ == '__main__':
    print(type(DEFAULT_CONFIG))
    print(DEFAULT_CONFIG)
    tmp = get_settings_from_file('config.json')
    print(tmp)
    print(type(tmp))
