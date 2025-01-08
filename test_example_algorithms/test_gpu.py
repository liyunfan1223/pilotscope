import torch
import torch.nn as nn
import torch.optim as optim
import time


# 定义一个复杂的卷积神经网络 (CNN) 模型
class ComplexCNN(nn.Module):
    def __init__(self):
        super(ComplexCNN, self).__init__()
        self.conv1 = nn.Conv2d(3, 64, kernel_size=3, stride=1, padding=1)
        self.conv2 = nn.Conv2d(64, 128, kernel_size=3, stride=1, padding=1)
        self.conv3 = nn.Conv2d(128, 256, kernel_size=3, stride=1, padding=1)
        self.fc1 = nn.Linear(256 * 4 * 4, 512)
        self.fc2 = nn.Linear(512, 10)
        self.relu = nn.ReLU()
        self.pool = nn.MaxPool2d(2, 2)

    def forward(self, x):
        x = self.relu(self.conv1(x))
        x = self.pool(x)
        x = self.relu(self.conv2(x))
        x = self.pool(x)
        x = self.relu(self.conv3(x))
        x = self.pool(x)
        x = torch.flatten(x, 1)
        x = self.relu(self.fc1(x))
        x = self.fc2(x)
        return x


def train(model, device, optimizer, criterion, train_loader, epochs):
    model.to(device)
    start_time = time.time()
    for epoch in range(epochs):
        running_loss = 0.0
        for i, (inputs, targets) in enumerate(train_loader, 0):
            inputs, targets = inputs.to(device), targets.to(device)
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
        print(f"Epoch {epoch + 1}, Loss: {running_loss / len(train_loader)}")
    end_time = time.time()
    return end_time - start_time


def main():
    # 检查是否有可用的 GPU
    device = torch.device("cpu")
    print(f"Using device: {device}")

    # 定义超参数
    batch_size = 128
    epochs = 10
    learning_rate = 0.001

    # 准备数据，使用 CIFAR-10 数据集作为示例
    transform = torchvision.transforms.Compose(
        [torchvision.transforms.ToTensor(),
         torchvision.transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )
    trainset = torchvision.datasets.CIFAR10(root='./data', train=True,
                                    download=True, transform=transform)
    train_loader = torch.utils.data.DataLoader(trainset, batch_size=batch_size,
                                          shuffle=True, num_workers=2)

    # 初始化模型、损失函数和优化器
    model = ComplexCNN()
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    # 在 CPU 上进行训练
    if device == torch.device("cpu"):
        print("Training on CPU...")
        cpu_time = train(model, device, optimizer, criterion, train_loader, epochs)
        print(f"CPU 训练时间: {cpu_time} 秒")
    # 在 GPU 上进行训练
    else:
        print("Training on GPU...")
        gpu_time = train(model, device, optimizer, criterion, train_loader, epochs)
        print(f"GPU 训练时间: {gpu_time} 秒")
        print(f"GPU 比 CPU 快 {cpu_time / gpu_time} 倍")


if __name__ == "__main__":
    import torchvision
    main()