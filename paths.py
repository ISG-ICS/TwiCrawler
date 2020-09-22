import os

# root path of the project
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# directory for logs
LOG_DIR = os.path.join(ROOT_DIR, 'logs')

# dir for all configs
CONFIGS_DIR = os.path.join(ROOT_DIR, 'configs')

# path of the config file for connection to postgresql
DATABASE_CONFIG_PATH = os.path.join(CONFIGS_DIR, 'database.ini')
# path of the config file for twitter api
TWITTER_API_CONFIG_PATH = os.path.join(CONFIGS_DIR, 'twitter.ini')
# path of the config file for logging in task manager
TASK_MANAGER_LOG_CONFIG_PATH = os.path.join(CONFIGS_DIR, 'logger-conf.json')
GENERAL_LOG_CONFIG_PATH = os.path.join(CONFIGS_DIR, 'general-logger.conf')

# dir for all cache
CACHE_DIR = os.path.join(ROOT_DIR, 'cache')

TWITTER_TEXT_CACHE = os.path.join(CACHE_DIR, 'twitter.cache.pickle')

# dir for data backup
BACKUP_DIR = os.path.join(ROOT_DIR, 'backup')
