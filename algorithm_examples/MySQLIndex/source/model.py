import os
from time import time

import joblib
import numpy as np
import torch
import torch.nn as nn
import torch.optim
from torch.utils.data import DataLoader, random_split

from feature import SampleEntity
from tcnn.module import ConvTree, ActivationTreeWrap, LayerNormTree, DynamicPoolingTree
from tcnn.util import prepare_trees
from tqdm import tqdm
from algorithm_examples.utils import print_log, log_file_name
CUDA = torch.cuda.is_available()
num_gpus = torch.cuda.device_count()
GPU_LIST = [0]

print("CUDA availabilty:", CUDA)
print("Num Gpus:", num_gpus, "GPU_LIST", GPU_LIST)

torch.set_default_tensor_type(torch.DoubleTensor)
device = torch.device("cuda:0" if CUDA else "cpu")


def _nn_path(base):
    return os.path.join(base, "nn_weights")

def _feature_generator_path(base):
    return os.path.join(base, "feature_generator")

def _input_feature_dim_path(base):
    return os.path.join(base, "input_feature_dim")

def collate_fn(x):
    trees = []
    targets = []

    for tree, target in x:
        trees.append(tree)
        targets.append(target)

    targets = torch.tensor(targets)
    return trees, targets

def collate_pairwise_fn(x):
    trees1 = []
    trees2 = []
    labels = []

    for tree1, tree2, label in x:
        trees1.append(tree1)
        trees2.append(tree2)
        labels.append(label)
    return trees1, trees2, labels


def transformer(x: SampleEntity):
    return x.get_feature()

def left_child(x: SampleEntity):
    return x.get_left()

def right_child(x: SampleEntity):
    return x.get_right()


class MySQLIndexNet(nn.Module):
    def __init__(self, input_feature_dim) -> None:
        super(MySQLIndexNet, self).__init__()
        self.input_feature_dim = input_feature_dim
        self._cuda = False
        self.device = None

        self.tree_conv = nn.Sequential(
            ConvTree(self.input_feature_dim, 256),
            LayerNormTree(),
            ActivationTreeWrap(nn.LeakyReLU()),
            ConvTree(256, 128),
            LayerNormTree(),
            ActivationTreeWrap(nn.LeakyReLU()),
            ConvTree(128, 64),
            LayerNormTree(),
            DynamicPoolingTree(),
            nn.Linear(64, 32),
            nn.LeakyReLU(),
            nn.Linear(32, 1)
        )

    def forward(self, trees):
        return self.tree_conv(trees)

    def build_trees(self, feature):
        return prepare_trees(feature, transformer, left_child, right_child, cuda=self._cuda, device=self.device)

    def cuda(self, device):
        self._cuda = True
        self.device = device
        return super().cuda()


class MySQLModel():
    def __init__(self, feature_generator) -> None:
        self._net = None
        self._feature_generator = feature_generator
        self._input_feature_dim = None
        self._model_parallel = None

    def load(self, path):
        with open(_input_feature_dim_path(path), "rb") as f:
            self._input_feature_dim = joblib.load(f)

        self._net = MySQLIndexNet(self._input_feature_dim)
        self._net.eval()
        if CUDA:
            self._net.cuda(device)
            self._net.load_state_dict(torch.load(_nn_path(path)))
            self._net = torch.nn.DataParallel(
                self._net, device_ids=GPU_LIST)
        else:
            self._net.load_state_dict(torch.load(
                _nn_path(path), map_location=torch.device('cpu')))

        with open(_feature_generator_path(path), "rb") as f:
            self._feature_generator = joblib.load(f)

    def save(self, path):
        os.makedirs(path, exist_ok=True)

        if CUDA:
            torch.save(self._net.module.state_dict(), _nn_path(path))
        else:
            torch.save(self._net.state_dict(), _nn_path(path))

        with open(_feature_generator_path(path), "wb") as f:
            joblib.dump(self._feature_generator, f)
        with open(_input_feature_dim_path(path), "wb") as f:
            joblib.dump(self._input_feature_dim, f)

    def fit(self, X, Y, pre_training=False, num_epochs = 100):
        if isinstance(Y, list):
            Y = np.array(Y)
            Y = Y.reshape(-1, 1)

        batch_size = 64
        if CUDA:
            batch_size = batch_size * len(GPU_LIST)

        pairs = []
        for i in range(len(Y)):
            pairs.append((X[i], Y[i]))
        dataset = DataLoader(pairs,
                             batch_size=batch_size,
                             shuffle=True,
                             collate_fn=collate_fn)

        if not pre_training:
            # # determine the initial number of channels
            input_feature_dim = len(X[0].get_feature())
            print("input_feature_dim:", input_feature_dim)

            self._net = MySQLIndexNet(input_feature_dim)
            self._input_feature_dim = input_feature_dim
            if CUDA:
                self._net = self._net.cuda(device)
                self._net = torch.nn.DataParallel(
                    self._net, device_ids=GPU_LIST)
                self._net.cuda(device)

        optimizer = None
        if CUDA:
            optimizer = torch.optim.Adam(self._net.module.parameters())
            optimizer = nn.DataParallel(optimizer, device_ids=GPU_LIST)
        else:
            optimizer = torch.optim.Adam(self._net.parameters())

        loss_fn = torch.nn.MSELoss()
        losses = []
        start_time = time()
        for epoch in range(num_epochs):
            loss_accum = 0
            for x, y in tqdm(dataset):
                if CUDA:
                    y = y.cuda(device)

                tree = None
                if CUDA:
                    tree = self._net.module.build_trees(x)
                else:
                    tree = self._net.build_trees(x)

                y_pred = self._net(tree)
                loss = loss_fn(y_pred, y)
                loss_accum += loss.item()

                if CUDA:
                    optimizer.module.zero_grad()
                    loss.backward()
                    optimizer.module.step()
                else:
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

            loss_accum /= len(dataset)
            losses.append(loss_accum)

            print("Epoch", epoch, "training loss:", loss_accum)
        print("training time:", time() - start_time, "batch size:", batch_size)

    def predict(self, x):
        if CUDA:
            self._net = self._net.cuda(device)

        if not isinstance(x, list):
            x = [x]

        tree = None
        if CUDA:
            tree = self._net.module.build_trees(x)
        else:
            tree = self._net.build_trees(x)

        pred = self._net(tree).cpu().detach().numpy()
        return pred

class MySQLModelPairWise(MySQLModel):
    def __init__(self, feature_generator) -> None:
        super().__init__(feature_generator)

    def fit(self, X1, X2, Y1, Y2, pre_training=False, num_epochs = 100):
        assert len(X1) == len(X2) and len(Y1) == len(Y2) and len(X1) == len(Y1)
        if isinstance(Y1, list):
            Y1 = np.array(Y1)
            Y1 = Y1.reshape(-1, 1)
        if isinstance(Y2, list):
            Y2 = np.array(Y2)
            Y2 = Y2.reshape(-1, 1)

        # # determine the initial number of channels
        if not pre_training:
            input_feature_dim = len(X1[0].get_feature())
            print("input_feature_dim:", input_feature_dim)

            self._net = MySQLIndexNet(input_feature_dim)
            self._input_feature_dim = input_feature_dim
            if CUDA:
                self._net = self._net.cuda(device)
                self._net = torch.nn.DataParallel(
                    self._net, device_ids=GPU_LIST)
                self._net.cuda(device)

        pairs = []
        for i in range(len(X1)):
            # y = None
            # if Y1[i] * 0.8 > Y2[i]:
            #     y = 1.0
            # elif Y2[i] * 0.8 > Y1[i]:
            #     y = 0.0
            # else:
            #     y = 0.5
            y = 1.0 if Y1[i] > Y2[i] else 0.0
            pairs.append((X1[i], X2[i], y))

        batch_size = 64
        if CUDA:
            batch_size = batch_size * len(GPU_LIST)

        num_total = len(pairs)
        num_train = int(num_total * 0.8)
        num_val = num_total - num_train

        print("Preparing training and validating dataset for {} pairs".format(num_total))
        # 使用 torch.utils.data.random_split 分割数据集
        train_pairs, val_pairs = random_split(pairs, [num_train, num_val])

        # 创建 DataLoader 用于训练集
        dataset = DataLoader(train_pairs,
                        batch_size=batch_size,
                        shuffle=True,
                        collate_fn=collate_pairwise_fn)

        # 创建 DataLoader 用于验证集
        dataset_validation = DataLoader(val_pairs,
                                    batch_size=batch_size,
                                    shuffle=True,
                                    collate_fn=collate_pairwise_fn)
        batched_x1 = []
        batched_x2 = []
        batched_y = []
        for x1, x2, label in dataset:
            batched_x1.append(prepare_trees(x1, transformer, left_child, right_child, cuda=CUDA, device=device))
            batched_x2.append(prepare_trees(x2, transformer, left_child, right_child, cuda=CUDA, device=device))
            batched_y.append(label)

        dataset = list(zip(batched_x1, batched_x2, batched_y))

        batched_x1_val = []
        batched_x2_val = []
        batched_y_val = []
        for x1, x2, label in dataset_validation:
            batched_x1_val.append(prepare_trees(x1, transformer, left_child, right_child, cuda=CUDA, device=device))
            batched_x2_val.append(prepare_trees(x2, transformer, left_child, right_child, cuda=CUDA, device=device))
            batched_y_val.append(label)
        dataset_validation = list(zip(batched_x1_val, batched_x2_val, batched_y_val))

        optimizer = torch.optim.Adam(self._net.parameters())
        bce_loss_fn = torch.nn.BCELoss()

        losses = []
        validation_losses = []
        sigmoid = nn.Sigmoid()
        start_time = time()
        for epoch in range(num_epochs):
            loss_accum = 0
            acc_accum = 0
            for x1, x2, label in tqdm(dataset):
                tree_x1, tree_x2 = x1, x2
                # if CUDA:
                #     tree_x1 = self._net.module.build_trees(x1)
                #     tree_x2 = self._net.module.build_trees(x2)
                # else:
                #     tree_x1 = self._net.build_trees(x1)
                #     tree_x2 = self._net.build_trees(x2)

                # pairwise
                y_pred_1 = self._net(tree_x1)
                y_pred_2 = self._net(tree_x2)
                diff = y_pred_1 - y_pred_2
                prob_y = sigmoid(diff)

                label_y = torch.tensor(np.array(label).reshape(-1, 1))
                if CUDA:
                    label_y = label_y.cuda(device)

                loss = bce_loss_fn(prob_y, label_y)
                loss_accum += loss.item()

                for y1, y2, y_label in zip(y_pred_1.cpu(), y_pred_2.cpu(), label_y.cpu()):
                    # y = None
                    # if y1 * 0.8 > y2:
                    #     y = 1.0
                    # elif y2 * 0.8 > y1:
                    #     y = 0.0
                    # else:
                    #     y = 0.5
                    y = 1.0 if y1 > y2 else 0.0
                    if y == y_label:
                        acc_accum += 1

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            loss_accum /= len(dataset)
            acc_accum /= num_train
            losses.append(loss_accum)
            
            # 验证集验证
            val_loss_accum = 0
            val_acc_accum = 0
            with torch.no_grad():  # 确保在验证过程中不计算梯度，不更新模型
                for x1_val, x2_val, label_val in tqdm(dataset_validation):
                    tree_x1_val, tree_x2_val = x1_val, x2_val
                    # if CUDA:
                    #     tree_x1_val = self._net.module.build_trees(x1_val)
                    #     tree_x2_val = self._net.module.build_trees(x2_val)
                    # else:
                    #     tree_x1_val = self._net.build_trees(x1_val)
                    #     tree_x2_val = self._net.build_trees(x2_val)

                    # pairwise
                    y_pred_1_val = self._net(tree_x1_val)
                    y_pred_2_val = self._net(tree_x2_val)
                    diff_val = y_pred_1_val - y_pred_2_val
                    prob_y_val = sigmoid(diff_val)

                    label_y_val = torch.tensor(np.array(label_val).reshape(-1, 1))
                    if CUDA:
                        label_y_val = label_y_val.cuda(device)

                    val_loss = bce_loss_fn(prob_y_val, label_y_val)
                    val_loss_accum += val_loss.item()

                    for y1, y2, y_label in zip(y_pred_1_val.cpu(), y_pred_2_val.cpu(), label_y_val.cpu()):
                        # y = None
                        # if y1 * 0.8 > y2:
                        #     y = 1.0
                        # elif y2 * 0.8 > y1:
                        #     y = 0.0
                        # else:
                        #     y = 0.5
                        y = 1.0 if y1 > y2 else 0.0
                        if y == y_label:
                            val_acc_accum += 1

            val_loss_accum /= len(dataset_validation)
            val_acc_accum /= num_val
            validation_losses.append(val_loss_accum)
            print_log("Epoch {}/{} training loss: {} training acc: {} validation loss: {} validation acc: {}".format(epoch + 1, num_epochs, loss_accum, acc_accum * 100, val_loss_accum, val_acc_accum * 100), log_file_name, True)

        print("training time:", time() - start_time, "batch size:", batch_size)
        