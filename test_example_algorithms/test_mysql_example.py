import sys

sys.path.append("../")
sys.path.append("../algorithm_examples/MySQLIndex/source")
import unittest
import json

from pilotscope.Common.Util import pilotscope_exit
from pilotscope.Common.Drawer import Drawer
from pilotscope.PilotConfig import PilotConfig, PostgreSQLConfig, MySQLConfig
from pilotscope.Common.TimeStatistic import TimeStatistic
from algorithm_examples.utils import load_test_sql
from algorithm_examples.MySQLIndex.MySQLPresetScheduler import get_mysql_preset_scheduler
from algorithm_examples.ExampleConfig import get_time_statistic_img_path
from pilotscope.DBController.MySQLController import MySQLController

import random
import torch
import numpy as np

SEED = 42

random.seed(SEED)
# 设置 PyTorch 的随机种子
torch.manual_seed(SEED)
# 如果使用 CUDA 设备，设置 GPU 的随机种子
if torch.cuda.is_available():
    torch.cuda.manual_seed(SEED)
    torch.cuda.manual_seed_all(SEED)
# 设置 numpy 的随机种子
np.random.seed(42)
# 禁用 PyTorch 的 cudnn 中的非确定性算法
torch.backends.cudnn.deterministic = True

# class MySQLTest(unittest.TestCase):
#     def setUp(self):
#         self.config: MySQLConfig = MySQLConfig()

#     def test_tpcds(self):
#         config = self.config
#         scheduler = get_mysql_preset_scheduler(config, enable_collection=True, enable_training=True, num_collection=10, num_epoch=1)
        
        # try:
        #     config = self.config
        #     scheduler = get_lero_preset_scheduler(config, enable_collection=True, enable_training=True, num_collection=10, num_epoch=1)
        #     print("start to test sql")
        #     sqls = load_test_sql(config.db)
        #     for i, sql in enumerate(sqls):
        #         print("current is the {}-th sql, and it is {}".format(i, sql))
        #         TimeStatistic.start('Lero')
        #         scheduler.execute(sql)
        #         TimeStatistic.end('Lero')
        #     name_2_value = TimeStatistic.get_sum_data()
        #     Drawer.draw_bar(name_2_value, get_time_statistic_img_path(self.algo, self.config.db), is_rotation=False)
        # finally:
        #     pilotscope_exit()


if __name__ == '__main__':
    config = MySQLConfig()
    scheduler = get_mysql_preset_scheduler(config, enable_collection=False, enable_training=True, num_collection=1000, num_epoch=100, num_training=10000)

# if __name__ == "__main__":
#     controller = MySQLController(MySQLConfig())
#     controller._connect_if_loss()
#     result = controller.explain_physical_plan("""
#         SELECT s.id, s.name, s.score, c.course_name
#         FROM student s
#         JOIN course c ON s.id = c.student_id;
#         """)
#     json_obj = json.loads(result)
#     print("Result:", result)
#     print("JSON:", json_obj)