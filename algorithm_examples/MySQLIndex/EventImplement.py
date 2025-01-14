import json
import time

from pandas import DataFrame

# from algorithm_examples.Lero.LeroPilotAdapter import CardsPickerModel
# from algorithm_examples.Lero.source.train import training_pairwise_pilot_score, get_training_pair
from algorithm_examples.MySQLIndex.source.train import training_pairwise_pilot_score, get_training_pair
from algorithm_examples.utils import load_training_sql, print_log, log_file_name
from pilotscope.DBController.BaseDBController import BaseDBController
from pilotscope.DBInteractor.PilotDataInteractor import PilotDataInteractor
from pilotscope.DataManager.DataManager import DataManager
from pilotscope.PilotConfig import PilotConfig
from pilotscope.PilotEvent import PeriodicModelUpdateEvent, PretrainingModelEvent, QueryFinishEvent
from pilotscope.PilotModel import PilotModel
from pilotscope.PilotTransData import PilotTransData
from algorithm_examples.MySQLIndex.source.feature import FeatureGenerator
from algorithm_examples.MySQLIndex.MySQLIndexSelector import MySQLIndexSelector
from sklearn.model_selection import train_test_split
import numpy as np

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
            sql_2_plans[sql].append(plan_json)

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
        self.better_count = 0
        self.similar_count = 0
        self.worse_count = 0
        self.at_least_one_better_count = 0
        self.at_least_one_better_excellent_count = 0
        self.total_count = 0

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

            default_time = 1000000
            best_time_with_hints = 1000000
            best_hint = str()
            temp_value_list = []
            for extended_sql, hint in zip(extended_sqls, hints):
                self.pilot_data_interactor.pull_physical_plan()
                self.pilot_data_interactor.pull_execution_time()
                data: PilotTransData = self.pilot_data_interactor.execute(extended_sql)
                if data is None:
                    # print(f"Warning: timeout in collecting data with hint {hint}. Try to enlarge 'timeout' in config to collect.")
                    self.pilot_data_interactor.pull_physical_plan()
                    data: PilotTransData = self.pilot_data_interactor.execute(extended_sql)
                    data.execution_time = self.config.sql_execution_timeout * 2

                plan = data.physical_plan
                column_2_value = {}
                column_2_value["sql"] = sql
                column_2_value["plan"] = plan
                column_2_value["time"] = data.execution_time
                column_2_value["hint"] = hint
                if hint == "":
                    default_time = min(default_time, data.execution_time)
                elif data.execution_time < best_time_with_hints:
                    best_time_with_hints = data.execution_time
                    best_hint = hint
                    
                # print("Execution time:", data.execution_time, "Hint:", hint)
                # finish, new_cards = cards_picker.get_cards()
                # scale_subquery_2_card = {sq : new_card for sq, new_card in zip(subquery_2_card.keys(), new_cards)}
                column_2_value_list.append(column_2_value)
                temp_value_list.append(column_2_value)
            
            self.total_count += 1
            if best_time_with_hints < default_time * 0.8:
                self.at_least_one_better_count += 1
                if best_time_with_hints < default_time * 0.2:
                    self.at_least_one_better_excellent_count += 1
            for column_2_value in temp_value_list:
                if column_2_value["hint"] == "":
                    continue

                if column_2_value["time"] < default_time * 0.8:
                    self.better_count += 1
                elif column_2_value["time"] * 0.8 > default_time:
                    self.worse_count += 1
                else:
                    self.similar_count += 1

            
            accumulative_total = self.better_count + self.similar_count + self.worse_count
            print_log("best time with hints: {:.4f}, default time: {:.4f}, best hints: {}".format(best_time_with_hints, default_time, best_hint), log_file_name, True)
            print_log("Accumulative better rate: {:.2f}% ({}/{}), similar rate: {:.2f}% ({}/{}), worse rate: {:.2f}% ({}/{})".format(self.better_count / accumulative_total * 100, self.better_count, accumulative_total,
                self.similar_count / accumulative_total * 100, self.similar_count, accumulative_total, self.worse_count / accumulative_total * 100, self.worse_count, accumulative_total), log_file_name, True)
            print_log("At least one better rate: {:.2f}% ({}/{}), excellent: {:.2f}% ({}/{})".format(self.at_least_one_better_count / self.total_count * 100, 
                self.at_least_one_better_count, self.total_count, self.at_least_one_better_excellent_count / self.total_count * 100, self.at_least_one_better_excellent_count, self.total_count), log_file_name, True)
            
            table = self.data_saving_table
            train_data_manager.save_data_batch(table, temp_value_list)
            print("{} records are written into table {}".format(len(temp_value_list), table))

        return column_2_value_list, True

    def custom_model_training(self, bind_pilot_model, db_controller: BaseDBController,
                              data_manager: DataManager):
        data: DataFrame = data_manager.read_all(self.data_saving_table)
        # if self.num_training > 0:
        #     data = data[:self.num_training]

        sqls = list(data["sql"].unique())
        if self.num_training > 0:
            sqls = sqls[:self.num_training]
        # 将 sql 按 8:2划分训练和测试集
        train_sqls, test_sqls = train_test_split(sqls, test_size=0.2, random_state=42)

        data_train = data[data["sql"].isin(train_sqls)]
        data_test = data[data["sql"].isin(test_sqls)]

        print(f"Train model on {data_train.shape[0]} plans")
        plans1, plans2 = extract_plan_pairs(data_train)
        mysql_model = training_pairwise_pilot_score(bind_pilot_model, plans1, plans2, self.num_epoch)

        print(f"Test model on {data_test.shape[0]} plans")

        speed_up_sum = 0
        counter = 0
        better_counter = 0
        worse_counter = 0
        similar_counter = 0
        explore_from_table = False

        total_selected_time = 0
        total_default_time = 0
        for i, sql in zip(range(len(test_sqls)), test_sqls):
            self.pilot_data_interactor.pull_possible_keys()
            self.pilot_data_interactor.pull_physical_plan()
            data: PilotTransData = self.pilot_data_interactor.execute(sql)
            feature_generator = FeatureGenerator()
            tables = feature_generator.get_all_table_to_ignore(data.physical_plan)
            index_selector = MySQLIndexSelector()
            if explore_from_table:
                extended_sqls = []
                hints = list(data_test.loc[(data_test["sql"] == sql)]["hint"])
                for hint in hints:
                    extended_sqls.append(index_selector.CombineSqlWithHints(sql, hint))
            else:
                extended_sqls, hints = index_selector.GenerateSQLsWithHints(sql, data.possible_keys, len(tables))

            feature_trees = []
            physical_plans = []
            for extended_sql, hint in zip(extended_sqls, hints):
                self.pilot_data_interactor.pull_physical_plan()
                data: PilotTransData = self.pilot_data_interactor.execute(extended_sql)
                physical_plan = data.physical_plan
                physical_plans.append(physical_plan)
            X, Y = mysql_model._feature_generator.transform(physical_plans)
            pred = mysql_model.predict(X)
            best_idx = np.argmin(pred)
            print_log("Model select hint for {}-th SQL: {}".format(i + 1, hints[best_idx]), log_file_name, True)

            if explore_from_table:
                selected_time = list(data_test.loc[(data_test["sql"] == sql) & (data_test["hint"] == hints[best_idx])]["time"])
                if len(selected_time) > 1:
                    selected_time = min(selected_time)
                else:
                    selected_time = selected_time[0]
                default_time = list(data_test.loc[(data_test["sql"] == sql) & (data_test["hint"] == "")]["time"])
                default_time = min(default_time)
            else:
                default_time_list = []
                for i in range(3):
                    self.pilot_data_interactor.pull_execution_time()
                    data = self.pilot_data_interactor.execute(sql)
                    if data is None:
                        default_time = self.config.sql_execution_timeout * 2
                    else:
                        default_time = data.execution_time
                    default_time_list.append(default_time)
                default_time = sorted(default_time_list)[1]

                selected_time_list = []
                for i in range(3):
                    self.pilot_data_interactor.pull_execution_time()
                    data = self.pilot_data_interactor.execute(index_selector.CombineSqlWithHints(sql, hints[best_idx]))
                    if data is None:
                        selected_time = self.config.sql_execution_timeout * 2
                    else:
                        selected_time = data.execution_time
                    selected_time_list.append(selected_time)
                selected_time = sorted(selected_time_list)[1]


            possible_times = list(data_test.loc[(data_test["sql"] == sql)]["time"])
            best_possible_time = min(possible_times)

            total_selected_time += selected_time
            total_default_time += default_time

            # speed_up_sum += default_time / selected_time
            if selected_time * 0.8 > default_time:
                worse_counter += 1
            elif default_time * 0.8 > selected_time:
                better_counter += 1
            else:
                similar_counter += 1

            counter += 1

            print_log("Execution speed up: {:.2f}%({:.4f}s/{:.4f}s), Best possible in table: {:.4f}s.".format(default_time / selected_time * 100, default_time, selected_time,
                best_possible_time), log_file_name, True)
            print_log("Total speed up: {:.2f}%({:.4f}s/{:.4f}s) Better: {:.2f}%({}/{}) Worse: {:.2f}%({}/{}) Similar: {:.2f}%({}/{})".format(
                total_default_time / total_selected_time * 100, total_default_time, total_selected_time,
                better_counter / counter * 100, better_counter, counter,
                worse_counter / counter * 100, worse_counter, counter, similar_counter / counter * 100, similar_counter, counter), log_file_name, True)
            # mysql_model.predict
                # mysql_model.predict
        return mysql_model
