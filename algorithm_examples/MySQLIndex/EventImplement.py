import json

from pandas import DataFrame

# from algorithm_examples.Lero.LeroPilotAdapter import CardsPickerModel
# from algorithm_examples.Lero.source.train import training_pairwise_pilot_score, get_training_pair
from algorithm_examples.MySQLIndex.source.train import training_pairwise_pilot_score, get_training_pair
from algorithm_examples.utils import load_training_sql
from pilotscope.DBController.BaseDBController import BaseDBController
from pilotscope.DBInteractor.PilotDataInteractor import PilotDataInteractor
from pilotscope.DataManager.DataManager import DataManager
from pilotscope.PilotConfig import PilotConfig
from pilotscope.PilotEvent import PeriodicModelUpdateEvent, PretrainingModelEvent, QueryFinishEvent
from pilotscope.PilotModel import PilotModel
from pilotscope.PilotTransData import PilotTransData
from algorithm_examples.MySQLIndex.source.feature import FeatureGenerator
from algorithm_examples.MySQLIndex.MySQLIndexSelector import MySQLIndexSelector

def extract_plan_pairs(data: DataFrame):
    sql_2_plans = {}
    sqls = list(data["sql"].unique())
    for sql in sqls:
        if sql not in sql_2_plans:
            sql_2_plans[sql] = []
        rows = data[data["sql"] == sql]
        for idx, row in rows.iterrows():
            plan_json = json.loads(row["plan"])
            plan_json["Execution Time"] = row["time"]
            sql_2_plans[sql].append(json.dumps(plan_json))

    # build pair
    plans1 = []
    plans2 = []
    # for i in range(len(sql_2_plans)):
    #     for j in range(i + 1, len(sql_2_plans)):
    #         plans1 += sql_2_plans[sqls[i]]
    #         plans2 += sql_2_plans[sqls[j]]
    for sql in sqls:
        plans = sql_2_plans[sql]
        if len(plans) == 1:
            continue
        p1, p2 = get_training_pair(plans)
        plans1 += p1
        plans2 += p2
    return plans1, plans2


class MySQLIndexPretrainingModelEvent(PretrainingModelEvent):

    def __init__(self, config: PilotConfig, bind_pilot_model: PilotModel, data_saving_table, enable_collection=True,
                 enable_training=True, num_collection = -1, num_training = -1, num_epoch = 100):
        super().__init__(config, bind_pilot_model, data_saving_table, enable_collection, enable_training)
        self.sqls = []
        self.pilot_data_interactor = PilotDataInteractor(self.config)
        self.num_collection = num_collection
        self.num_training = num_training
        self.num_epoch = num_epoch

    def load_sql(self):
        self.sqls = load_training_sql(self.config.db)

    def iterative_data_collection(self, db_controller: BaseDBController, train_data_manager: DataManager):
        print("start to collect data for pretraining")
        self.load_sql()

        column_2_value_list = []
        if self.num_collection > 0:
            train_sqls = self.sqls[:self.num_collection]
        else:
            train_sqls = self.sqls
        
        for i, sql in enumerate(train_sqls):
            data: PilotTransData = self.pilot_data_interactor.pull_physical_plan()
            data: PilotTransData = self.pilot_data_interactor.execute(sql)
        print("Successfully validate all {} sqls by explaining".format(len(train_sqls)))

        for i, sql in enumerate(train_sqls):
            print("current is {}-th sql, and total sqls is {}".format(i + 1, len(train_sqls)))

            self.pilot_data_interactor.pull_possible_keys()
            self.pilot_data_interactor.pull_physical_plan()
            data: PilotTransData = self.pilot_data_interactor.execute(sql)
            feature_generator = FeatureGenerator()
            tables = feature_generator.get_all_table_to_ignore(data.physical_plan)
            index_selector = MySQLIndexSelector()
            extended_sqls, hints = index_selector.GenerateSQLsWithHints(sql, data.possible_keys, len(tables))
            for extended_sql, hint in zip(extended_sqls, hints):
                self.pilot_data_interactor.pull_physical_plan()
                self.pilot_data_interactor.pull_execution_time()
                data: PilotTransData = self.pilot_data_interactor.execute(extended_sql)
                if data is None:
                    print(f"Warning: timeout in collecting data with hint {hint}. Try to enlarge 'timeout' in config to collect.")
                    continue
                plan = data.physical_plan
                column_2_value = {}
                column_2_value["sql"] = sql
                column_2_value["plan"] = plan
                column_2_value["time"] = data.execution_time
                print("Execution time:", data.execution_time, "Hint:", hint)
                # finish, new_cards = cards_picker.get_cards()
                # scale_subquery_2_card = {sq : new_card for sq, new_card in zip(subquery_2_card.keys(), new_cards)}
                column_2_value_list.append(column_2_value)
        return column_2_value_list, True

    def custom_model_training(self, bind_pilot_model, db_controller: BaseDBController,
                              data_manager: DataManager):
        data: DataFrame = data_manager.read_all(self.data_saving_table)
        if self.num_training > 0:
            data = data[:self.num_training]
        print(f"Train mysql on {data.shape[0]} plans")
        plans1, plans2 = extract_plan_pairs(data)
        mysql_model = training_pairwise_pilot_score(bind_pilot_model, plans1, plans2, self.num_epoch)
        return mysql_model
