import sys

from pilotscope.DataManager.DataManager import DataManager

sys.path.append("../")
sys.path.append("../algorithm_examples/MysqlIndex/source")

from pilotscope.Factory.SchedulerFactory import SchedulerFactory
from pilotscope.PilotModel import PilotModel
from pilotscope.PilotScheduler import PilotScheduler
# from algorithm_examples.Lero.EventImplement import LeroPretrainingModelEvent, LeroPeriodicCollectEvent, \
#     LeroPeriodicModelUpdateEvent
from algorithm_examples.MySQLIndex.EventImplement import MySQLIndexPretrainingModelEvent
# from algorithm_examples.Lero.LeroParadigmCardAnchorHandler import LeroCardPushHandler
from algorithm_examples.MySQLIndex.MySQLPilotModel import MySQLIndexPilotModel

def get_mysql_preset_scheduler(config, enable_collection, enable_training, num_collection = -1, num_training = -1, num_epoch = 100) -> PilotScheduler:
    if type(enable_collection) == str:
        enable_collection = eval(enable_collection)
    if type(enable_training) == str:
        enable_training = eval(enable_training)
    if type(num_collection) == str:
        num_collection = int(num_collection)
    if type(num_training) == str:
        num_training = int(num_training)
    if type(num_epoch) == str:
        num_epoch = int(num_epoch)

    model_name = "mysql"
    test_data_table = "{}_test_data_table".format(model_name)
    pretraining_data_table = f"mysql_pretraining_collect_data_for_{config.db}"

    data_manager = DataManager(config)
    if enable_collection: # if enable_collection, drop old data and collect new data. otherwise use old data to train.
        data_manager.remove_table_and_tracker(pretraining_data_table)

    mysql_pilot_model: PilotModel = MySQLIndexPilotModel(model_name)
    mysql_pilot_model.load_model()

    scheduler: PilotScheduler = SchedulerFactory.create_scheduler(config)
    scheduler.register_required_data(test_data_table, pull_execution_time=True, pull_physical_plan=True)

    pretraining_event = MySQLIndexPretrainingModelEvent(config, mysql_pilot_model, pretraining_data_table,
                                                  enable_collection=enable_collection, enable_training=enable_training, num_collection = num_collection,\
                                                  num_training = num_training, num_epoch = num_epoch)
    scheduler.register_events([pretraining_event])

    scheduler.init()

    return scheduler
    # lero_pilot_model: PilotModel = LeroPilotModel(model_name)
    # lero_pilot_model.load_model()
    # lero_handler = LeroCardPushHandler(lero_pilot_model, config)

    # # core
    # scheduler: PilotScheduler = SchedulerFactory.create_scheduler(config)
    # scheduler.register_custom_handlers([lero_handler])
    # scheduler.register_required_data(test_data_table, pull_execution_time=True, pull_physical_plan=True)

    # # allow to pretrain model
    # pretraining_event = LeroPretrainingModelEvent(config, lero_pilot_model, pretraining_data_table,
    #                                               enable_collection=enable_collection, enable_training=enable_training, num_collection = num_collection,\
    #                                               num_training = num_training, num_epoch = num_epoch)
    # scheduler.register_events([pretraining_event])

    # # start
    # scheduler.init()
    # return scheduler

