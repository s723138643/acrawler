import json
import os.path

DEFAULT_CONFIG = {
        'crawler_header': {
            'User-Agent':'Mozilla/5.0 (X11; Linux x86_64; rv:42.0) Gecko/20100101 Firefox/43.0'
            },
        'cookiepath':'cookies.txt',
        'hostonly': True,
        'maxredirect': None,        # 默认不设置最大跳转数
        'maxdeep': None,            # 默认不设置最大爬取深度
        'download_threads': 1,
        'download_timeout': None,
        'parse_threads': 1,
        'get_queue_timeout': None,
        'max_memq_size': 30,
        'sqlite_task_path': 'task.db'
        }

def get_settings_from_file(path):
    settings = DEFAULT_CONFIG
    if os.path.isfile(path):
        with open(path, 'r') as f:
            tmp = json.load(f)
        settings.update(tmp)
    return settings

if __name__ == '__main__':
    print(type(DEFAULT_CONFIG))
    print(DEFAULT_CONFIG)
    tmp = get_settings_from_file('config.json')
    print(tmp)
    print(type(tmp))
