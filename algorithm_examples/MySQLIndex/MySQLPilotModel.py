import os

# from algorithm_examples.Lero.source.model import LeroModelPairWise
from pilotscope.DataManager.DataManager import DataManager
from pilotscope.PilotModel import PilotModel


class MySQLIndexPilotModel(PilotModel):

    def __init__(self, model_name):
        super().__init__(model_name)
        self.mysql_model_save_dir = "../algorithm_examples/ExampleData/MySQL/Model"
        self.model_path = os.path.join(self.mysql_model_save_dir, self.model_name)

    def train(self, data_manager: DataManager):
        print("enter MySQLPilotModel.train")

    def update(self, data_manager: DataManager):
        print("enter MySQLPilotModel.update")

    def save_model(self):
        self.model.save(self.model_path)

    def load_model(self):
        pass
        # try:
        #     mysql_model = LeroModelPairWise(None)
        #     mysql_model.load(self.model_path)
        # except FileNotFoundError:
        #     print("Can not load model. Lero model file not find, so init by random.")
        #     mysql_model = LeroModelPairWise(None)
        # self.model = lero_model
