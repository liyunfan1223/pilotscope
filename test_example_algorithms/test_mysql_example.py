import sys

sys.path.append("../")

import unittest
import json

from pilotscope.Common.Util import pilotscope_exit
from pilotscope.Common.Drawer import Drawer
from pilotscope.PilotConfig import PilotConfig, PostgreSQLConfig, MySQLConfig
from pilotscope.Common.TimeStatistic import TimeStatistic
from algorithm_examples.utils import load_test_sql
from algorithm_examples.Lero.LeroPresetScheduler import get_lero_preset_scheduler
from algorithm_examples.ExampleConfig import get_time_statistic_img_path
from pilotscope.DBController.MySQLController import MySQLController

if __name__ == "__main__":
    controller = MySQLController(MySQLConfig())
    controller._connect_if_loss()
    result = controller.explain_physical_plan("""
        SELECT s.id, s.name, s.score, c.course_name
        FROM student s
        JOIN course c ON s.id = c.student_id;
        """)
    json_obj = json.loads(result)
    print("Result:", result)
    print("JSON:", json_obj)