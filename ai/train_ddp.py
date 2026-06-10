"""
附加题3 · PyTorch 分布式数据并行 (DDP) 训练 MNIST —— CPU / gloo 后端
（此文件与镜像 group17/pytorch-ddp:v1 内 /app/train_ddp.py 一致，放入仓库便于查阅）

同一份脚本支持两种模式（由环境变量 WORLD_SIZE 决定）：
  · 单机基线   WORLD_SIZE=1      → 普通单进程训练
  · 分布式     WORLD_SIZE=2 + RANK/MASTER_ADDR/MASTER_PORT → DDP 数据并行

用 init_method='env://' 直接读取环境变量完成 rendezvous，无需 torchrun。
数据集已在构建镜像时烘焙到 /data（download=False，节点无公网）。
"""
import os
import time
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from torchvision import datasets, transforms

DATA_DIR = os.environ.get("DATA_DIR", "/data")
EPOCHS = int(os.environ.get("EPOCHS", "2"))
BATCH = int(os.environ.get("BATCH_SIZE", "64"))
LR = float(os.environ.get("LR", "1.0"))


class Net(nn.Module):
    """经典 MNIST CNN（约 1.2M 参数）。"""
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = F.relu(self.conv1(x))
        x = F.relu(self.conv2(x))
        x = F.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = F.relu(self.fc1(x))
        x = self.dropout2(x)
        return self.fc2(x)


def main():
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    rank = int(os.environ.get("RANK", "0"))
    distributed = world_size > 1

    threads = int(os.environ.get("THREADS", "0"))
    if threads > 0:
        torch.set_num_threads(threads)

    if distributed:
        dist.init_process_group(backend="gloo", init_method="env://",
                                world_size=world_size, rank=rank)
        print(f"[rank {rank}] DDP 初始化完成 world_size={world_size} "
              f"master={os.environ.get('MASTER_ADDR')}:{os.environ.get('MASTER_PORT')}",
              flush=True)

    torch.manual_seed(42)
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,)),
    ])
    train_ds = datasets.MNIST(DATA_DIR, train=True, download=False, transform=transform)
    test_ds = datasets.MNIST(DATA_DIR, train=False, download=False, transform=transform)

    if distributed:
        sampler = DistributedSampler(train_ds, num_replicas=world_size, rank=rank, shuffle=True)
        train_loader = DataLoader(train_ds, batch_size=BATCH, sampler=sampler, num_workers=2)
    else:
        sampler = None
        train_loader = DataLoader(train_ds, batch_size=BATCH, shuffle=True, num_workers=2)
    test_loader = DataLoader(test_ds, batch_size=1000, shuffle=False)

    model = Net()
    if distributed:
        model = DDP(model)  # CPU DDP：每步反向后对梯度做 AllReduce 求平均
    optimizer = torch.optim.Adadelta(model.parameters(), lr=LR)

    n_threads = torch.get_num_threads()
    if rank == 0:
        mode = f"DDP({world_size} workers)" if distributed else "单机(1 worker)"
        print(f"==== 开始训练 | 模式={mode} | epochs={EPOCHS} | batch/worker={BATCH} "
              f"| torch线程={n_threads} | 训练样本={len(train_ds)} ====", flush=True)

    if distributed:
        dist.barrier()
    t0 = time.time()
    model.train()
    for epoch in range(1, EPOCHS + 1):
        if sampler is not None:
            sampler.set_epoch(epoch)
        running = 0.0
        for i, (data, target) in enumerate(train_loader):
            optimizer.zero_grad()
            loss = F.cross_entropy(model(data), target)
            loss.backward()
            optimizer.step()
            running += loss.item()
            if rank == 0 and i % 100 == 0:
                print(f"  epoch {epoch} step {i:4d}  loss={loss.item():.4f}", flush=True)
        if rank == 0:
            print(f"  -> epoch {epoch} 平均loss={running/max(1,len(train_loader)):.4f}", flush=True)
    if distributed:
        dist.barrier()
    train_time = time.time() - t0

    acc = float("nan")
    if rank == 0:
        model.eval()
        correct = 0
        with torch.no_grad():
            for data, target in test_loader:
                pred = model(data).argmax(1)
                correct += (pred == target).sum().item()
        acc = 100.0 * correct / len(test_ds)
        total_samples = len(train_ds) * EPOCHS
        thr = total_samples / train_time
        print("============================================================", flush=True)
        print(f"##RESULT## world_size={world_size} epochs={EPOCHS} "
              f"train_time_s={train_time:.2f} throughput_samples_s={thr:.1f} "
              f"test_acc={acc:.2f}%", flush=True)
        print("============================================================", flush=True)

    if distributed:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
